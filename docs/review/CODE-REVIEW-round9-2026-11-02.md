# 第九轮：全量复审（静态）

- **基线**：`106a2e1`（main，`include/tcp-server-mt.h`，3400 行）
- **评审日期**：2026-11-02
- **性质**：纯静态评审。评审环境为 Windows，无法编译运行本头文件，因此本轮**没有任何条目携带运行时证据**。按本项目「无证据不定罪」的规则，所有条目均标记 `[需核实]`，结论一栏写的是推理链与证伪/证实方法，不是判决。
- **范围**：主头文件全量通读；重点放在前八轮的修复点上做回归推理（R5-7 的 push_back、R6-1 的最后所有者、R7 的 FIN 抢答、R8-1 的关闭预算、R8-3 的 N1 变体），以及它们修复后新引入的相邻代码。

| 条目 | 级别 | 结论 | 状态 |
|---|---|---|---|
| R9-1 `handle_task(close_task/drain_input_task)` 的提前 `reset()` 在 bad_alloc 下给 `drain_input_then_close` 留了一个 UAF 窗口 | P2 候选 | **成立（推理）**，是 R5-7 的同族残余 | `[需核实: Linux 故障注入]` |
| R9-2 message handler 抛异常杀死整个服务器，state handler 抛异常被吞掉 | P3 | **成立（设计不一致）** | 无需运行即可确认；是否改是取舍 |
| R9-3 关停路径上被丢弃的 send_task 不归还 `pending_bytes` 也不计数 | P4 | **成立但无害**（无泄漏，仅计数缺口） | 写理由即可 |
| R9-4 `close_requested` 在入队之后才置位，并发 send 可越过已排队的 close | P4 | **成立但语义可辩护** | 一行加固，可选 |

---

## 1. R9-1（P2 候选）：提前 reset 与 R5-7 留下的 bad_alloc 窗口

### 代码事实

两个任务处理器都是同一个写法（`tcp-server-mt.h:1455-1471`、`:1484-1491`）：

```cpp
inline void sub_reactor::handle_task(close_task& task) noexcept {
    auto it = conns_.find(id);
    if (it != conns_.end()) {
        std::shared_ptr<connection> c = it->second;
        connection* raw = c.get();
        c.reset();                       // ← 本帧不再持有引用
        drain_input_then_close(raw);     // ← raw 只靠 pending_closes_ 续命
    }
    flush_close_callbacks();
}
```

注释说明了意图：「the deferred close callbacks must not run while this frame holds the conn」。但回调只在 `flush_close_callbacks()` 里跑，而它在 `drain_input_then_close` **返回之后**才被调用——把 reset 从调用前挪到调用后，回调照样不会在本帧持有时运行。提前 reset 买到的东西是零。

它付出的东西在 `teardown_closed()` 的 catch 分支（`:2377-2387`）：

```cpp
try {
    closed_conn entry;
    entry.conn   = std::move(keep_alive);   // 引用先搬进 entry
    entry.id     = c->id;
    entry.peer   = c->peer;                 // string 拷贝，可抛
    pending_closes_.push_back(std::move(entry));  // vector 扩容，可抛
} catch (...) {
    close_callbacks_dropped.fetch_add(...);
    // entry 析构 → keep_alive 已 move 进 entry → 连接在此销毁
}
```

两个抛点（peer 拷贝、push_back）中的任何一个触发时，`conns_` 已擦除、shard 表已擦除、调用方的 `c` 已 reset——**连接对象在 catch 里被销毁**。而调用栈上方：

```cpp
const int served = handle_read(c);          // 内部可能走到上面的 teardown
if (c->is_closed.load(...)) return;         // :2245 —— 读已释放内存
...
c->close_input_bytes -= dealt;              // :2270 —— 写已释放内存
park_close(c, "requested");                 // :2311/:2321 —— 同上
```

`handle_read` 内部有四条路径可达 teardown（recv error、rbuf 满、parse 错误、finish_peer_closed 的三处），全部在 `drain_input_then_close` 的 `handle_read(c)` 调用之下。正常路径下这不构成问题——`pending_closes_` 里的 `entry.conn` 把对象续命到 flush 之后，这正是 `closed_conn` 存在的意义。问题只在通知分配失败的那一次。

### 与前史的对应

R5-7 修的是同一代码位置的同类问题（四个并行 vector 的部分插入），当时的原话是「On the close path, which is the one that gets walked when memory is tight」——关闭路径恰恰是内存紧张时会被走到的路径。单对象化修复了半个通知，但同一个 catch 分支 still 把连接本体在裸指针存活期内释放掉。本轮条目是那一轮的同族残余，不是新问题。

### 改法（一行级）

把两个 handler 的 reset 挪到调用之后，让本帧的 shared_ptr 覆盖整个 `drain_input_then_close`：

```cpp
std::shared_ptr<connection> c = it->second;
drain_input_then_close(c.get());
// c 在本作用域末尾、flush_close_callbacks() 之前析构
```

回调语义不变（flush 仍在持帧引用消失后运行），UAF 窗口关闭。`cleanup()`（`:1155-1158`）是同一种写法但调用后不再解引用 raw，不需要动。

### 证实/证伪方法

参照 `probe_teardown_alloc.cpp` 的既有套路：故障注入 `pending_closes_` 的分配器（或把 `closed_conn.peer` 换成注入型分配器的 string），在 close_task 触发的 drain 中途制造 teardown，ASan 下跑 `probe_close_budget` 的同类场景。预期：修复前 ASan 报 heap-use-after-free（读 `is_closed`），修复后干净。

---

## 2. R9-2（P3）：两种应用回调的异常，两种命运

### 代码事实

同一个头文件对「应用回调抛异常」给了两种处理：

- **conn_state_handler(true)**（`:1304-1308`）：专用 try/catch，吞掉，注释明说「nothing to undo, nothing to count」——R8-2 的处置。
- **message_handler**（`:2023-2024`）：`if (mh) mh(c->id, ...);` 无任何防护。异常沿 `parse_frames → handle_read → loop()` 的外层 `catch (...)` 一路上抛，结果是 `reactor_failures++`、`fail()`、该 reactor 线程退出；其余 reactor 在下一次循环检查 `is_running()` 时跟进退出。**一个 handler 的一次 throw = 整台服务器失效**。

close 回调是第三种命运：`flush_close_callbacks` 里裸跑，靠 `std::terminate` 执行契约（R8-2 顺带确认过）。

三种回调、三种后果（吞掉 / 杀全服 / 终止进程），而头文件对应用只说一句「must not throw」。R8-2 为 state handler 选择吞掉的理由——「异常本身已经回到写它的人手上」——对 message handler 一字不差地成立，但后者拿到的是最重的惩罚。

### 取舍

这不是「有 bug」，是「同一类契约违规的代价不一致」，运维侧表现为：一个本可被 `try/catch` 隔离的应用 bug 会把所有连接（包括健康连接）一起带走，且 `reactor_failures` 计数器无法区分「reactor 自己坏了」和「应用代码坏了」。

- 若要改：给 `mh(...)` 加 try/catch 并计数。计数器放哪是个真问题——`parse_errors` 语义不对，`reactor_failures` 语义也不对；按 R8-2 对「没有合适槽位就不加计数器」的先例，吞掉并依赖应用自己的日志也是一个自洽选项。
- 若不改：在 `set_message_handler` 的契约注释里把「抛异常 = 整服失效」写明，与 state handler 的「抛异常 = 被吞」并列，让不对称成为一个决定而不是一个意外。

---

## 3. R9-3（P4）：关停路径丢弃的 send_task 不归还 reservation

### 代码事实

`process_pending()` 的中途退出（`:1189-1195`，`!running_ || !is_running()` 时 break）和 `cleanup()` 的清队列（`:1143-1148`）都会直接销毁队列里剩余的 `send_task`。这些帧在 `send_to()` 里已经计入所属连接的 `pending_bytes`（`:3328-3331`），销毁时没有 `fetch_sub` 归还，也不计 `bytes_dropped` / `queue_drops`。

无泄漏：`pending_bytes` 是每连接计数，连接随即在 cleanup 里被 teardown 销毁，reservation 随对象一起消失，`discard_pending_output` 只管 wbuf 部分是正确分工（`:2107-2109` 的注释也已声明不管队列部分）。

缺口在计数：关停瞬间在飞的帧既不算送达也不算丢弃，`bytes_dropped` 低报。按本项目「lost work must be countable」的哲学这是一处例外，但它只在关停瞬间发生、且连接已死，写一句理由（像 R8-4 那样）比加计数器划算。**建议仅记录，不改代码。**

---

## 4. R9-4（P4）：`close_requested` 置位晚于入队

### 代码事实

`close_connection()`（`:3366-3373`）先入队 close_task，后置 `close_requested`：

```cpp
if (!c->owner->submit_close(conn_id)) { ... }
c->close_requested.store(true, ...);   // 入队成功之后才置位
```

`try_send_local` 的快路径靠 `close_requested` 回落到队列路径（`:1408`）。在入队与置位之间，owner 线程上一个正在运行的 handler 若对同一连接调 `send_to()`，快路径看到 `close_requested == false`，直接写 wbuf，**越过**已排队的 close_task；随后 park_close 的 drain 会把这帧送达。

后果方向是好的（该丢的帧被送达了，不是反过来），且只影响与 close 并发的 send——并发本身没有既定顺序，所以语义上可以辩护。但 `:416-419` 的注释声称这个标志保证「a send issued after close_connection() keeps the existing queue order」，当前的置位顺序并不能保证这一点（它保证的是「close_connection 返回之后」）。

### 改法（可选，一行级）

入队前置位，入队失败则复位：

```cpp
c->close_requested.store(true, std::memory_order_release);
if (!c->owner->submit_close(conn_id)) {
    c->close_requested.store(false, std::memory_order_release);
    close_requests_dropped.fetch_add(...);
    return false;
}
```

代价：与 close 并发的 send 从「大概率送达」变成「一律按已关闭丢弃并计数」——与 `Ok` 的契约更一致。**可选**；不改的话建议把注释里的保证改弱为实际保证的那个。

---

## 5. 本轮复核为「不成立/已关闭」的条目

为避免后续轮次重新提出：

- **R5-7 回归**：单对象 `closed_conn` 已消除部分插入；残余即 R9-1，范围窄得多。
- **R7（FIN 抢答）回归**：`deferred_round_kind` 三分态 + `pending_bytes` 守卫仍在，`finish_peer_closed` 的 case 2 deferral 只有一次，判断链完整。
- **R8-1 回归**：`drain_read_bytes` 的 FIFO 计费（`:2266-2272`）实现与文档一致；`close_input_bytes` 只在首次测量，`served > 0` 的进度要求能挡住半帧拖延。
- **R8-3（N1 变体）**：`handle_write` 的 FIN 分支前置条件（wbuf 空 + 三个 closing 来源的 FIFO 论证）本轮重推一遍，结论不变：今天无可达路径。
- **`finish_peer_closed` / `handle_write` 的 teardown 后路径**：所有 `teardown_closed` 调用点之后都不再解引用 `c`（loop 的 keep_alive 覆盖事件分发；唯二例外即 R9-1 的两个 task handler）。
- **`handler_worker` 的每连接串行**：`handler_active_` + ready 重排在两把锁的同一临界区内完成，串行保证成立。
- **`stop()` 重入**：`life_ == stopped` 早退；callback 内调用走 `request_stop()`，均正确。

## 6. 本轮未覆盖

- `verify/` 下 21 个 TU 与 CMake/CI：未评审（README 声明头文件是唯一交付物，套件是验证手段）。
- 任何需要 Linux 运行时的断言：本轮全部结论均为静态推理，R9-1 的证实必须先写探针。
