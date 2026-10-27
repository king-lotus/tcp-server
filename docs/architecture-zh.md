# tcp-server 架构与使用（当前实现）

对应实现：`include/tcp-server-mt.h`。单头文件、C++17、Linux（`epoll` + `eventfd`），
无第三方依赖。本文描述的是**当前代码的实际行为**，不是设计意图；两者不一致时以
头文件里的注释和代码为准。

---

## 1. 它是什么

- 一个服务端库：接收 TCP 连接、按 8 字节帧头切分消息、把完整帧交给应用回调。
- 一个头文件就是全部交付物：复制 `include/tcp-server-mt.h` 到你的工程即可，
  没有 `.cpp`、没有链接步骤。
- 只支持 Linux。头文件里直接 `#error`，因为 `epoll` 和 `eventfd` 是硬依赖。
- 不提供：TLS、客户端实现、重连、协议版本协商、跨平台抽象。

```cpp
#include "tcp-server-mt.h"   // -I<repo>/include -std=c++17 -pthread
industrial::multi_reactor_server server(cfg);
```

---

## 2. 总体架构

### 2.1 线程与所有权

```
应用线程（任意多个）
    send_to(id, ...) / close_connection(id) / connection_ids() / stats()
              │
              │ 查 shared_ptr<connection>，再把任务投递到连接所属 reactor
              ▼
┌─────────────────────────────────────────────────────────────┐
│ multi_reactor_server                                        │
│   cfg_ / stats_ / conns_（全局视图，shared_mutex 保护）      │
└───────────────┬─────────────────────────────┬───────────────┘
                │ round-robin (pick_reactor)   │
                ▼                             ▼
      ┌──────────────────┐          ┌──────────────────┐
      │ sub_reactor 0    │   ...    │ sub_reactor N-1  │
      │ 自己的线程        │          │ 自己的线程        │
      │ epoll fd         │          │ epoll fd         │
      │ eventfd 唤醒      │          │ eventfd 唤醒      │
      │ conns_/fd_index_ │          │ conns_/fd_index_ │
      │ 任务队列          │          │ 任务队列          │
      └────────┬─────────┘          └────────┬─────────┘
               │                             │
           若干 socket                   若干 socket

      ┌──────────────────────────────────────────────────┐
      │ accept 线程                                       │
      │ listen fd + epoll + eventfd                       │
      │ accept4（每轮最多 64 个）→ 准入检查 → 轮转交棒      │
      └──────────────────────────────────────────────────┘
```

- **一个 accept 线程 + N 个 sub-reactor**。`config::num_reactors == 0` 时取
  `std::thread::hardware_concurrency()`，上限 `config::k_max_reactors = 64`。
- **连接归属固定**：一条连接从注册到销毁只属于一个 reactor。`connection` 的非原子字段
  只由该 reactor 线程读写；应用线程只通过 `send_to` / `close_connection` 触碰
  原子字段，或通过 `connection_ids()` / `stats()` 读全局快照。
- **任务队列 + eventfd**：跨线程提交不是直接操作连接，而是把任务放进目标
  reactor 的队列，再写 eventfd 唤醒它。这保证连接状态始终由 owner 线程修改。
- **owner-thread 快速路径**：handler 本来就在连接所属 reactor 上，调用 `send_to`
  时可以直接写 wbuf，绕过全局锁、任务队列和 eventfd；只有非 owner 线程、其它
  reactor 的连接，或 wbuf 暂时没空间时才走队列。
- **可选 handler 池**：`config::handler_threads > 0` 时额外创建 N 条 worker 线程，
  只执行 `offload()` 提交的任务（见 4.1）。它们不属于任何 reactor，也不直接持有
  连接；任务里的 `send_to` 走队列路径回到 owner。
- **轮转分配**：`pick_reactor()` 用 `next_reactor_` 递增取模，连接均匀落到各
  reactor。`reactor_spread` 套件断言了这一点。

### 2.2 accept 线程

1. `epoll_wait` 监听 `listen_fd_` 和 `wake_fd_`。
2. 每轮最多 `accept4` 64 个连接，避免新连接洪峰饿死其它 fd。
3. 设置 `TCP_NODELAY`、`SO_KEEPALIVE`（`TCP_KEEPIDLE=30s`、
   `TCP_KEEPINTVL=10s`、`TCP_KEEPCNT=3`）。
4. 取对端地址字符串，分配递增的连接 id。
5. 准入：`max_connections` 用 `admission_ticket` 的 RAII 计数控制；超限直接 `rejected++`
   并关闭新 fd。
6. 轮转选 reactor，把 fd 所有权交给它；注册成功后才 `accepted++`，失败则
   `rejected++`。

`accepted` 统计的是“已经注册进 reactor 的连接”，不是“accept 返回的 fd”；
`max_connections` 的准入计数则覆盖从 accept 到连接销毁的整个生命周期。

### 2.3 reactor 主循环

每个 reactor 线程大致做这些事，顺序固定：

```
while (running && server->is_running()) {
    处理本 reactor 的待读连接（最多 64 条，每条 handle_read 有预算）
    发布 frames_recv_local_ → stats.frames_recv
    epoll_wait(..., read_head_ ? 0 : tick_ms)
    处理 EPOLLIN / EPOLLOUT / EPOLLHUP|EPOLLERR
    flush_close_callbacks()     // 延迟的关闭回调
    process_pending()          // 任务队列
    flush_close_callbacks()
    到 tick 点则 do_tick()     // 空闲与 linger 扫描
}
```

`read_head_` 非空时 `epoll_wait` 超时为 0：本 reactor 还有欠读的连接，处理完再等。
`do_tick()` 只在 `tick_ms` 到达时运行；空闲超时和关闭 linger 都按 tick 粒度检查。

---

## 3. 帧协议

线上是 8 字节大端帧头 + 载荷：

| 偏移 | 长度 | 字段 | 说明 |
|---|---|---|---|
| 0 | 2 | magic | 固定 `0xEB90`，大端 |
| 2 | 2 | type | `1=DATA`、`2=PING`、`3=PONG`，大端 |
| 4 | 4 | length | 载荷字节数，大端，最大 `65536` |
| 8 | length | payload | 原始字节，服务端不解释 |

- `FRAME_HEADER_SIZE = 8`
- `MAX_FRAME_PAYLOAD = 64 * 1024`
- `MAX_FRAME_SIZE = 8 + 65536 = 65544`

服务端对帧的处理：

| 收到 | 行为 |
|---|---|
| `PING` | 自动回一个 8 字节 `PONG`（无载荷），不经过应用回调 |
| `PONG` | 直接消费，忽略 |
| `DATA` 及其它非 PING/PONG 类型 | 把载荷交给 `message_handler(id, payload, len)` |
| magic 错 | `parse_errors++`，中止连接（RST） |
| `length > MAX_FRAME_PAYLOAD` | `parse_errors++`，中止连接（RST） |
| 帧不完整 | 留在 `rbuf`，等下一次 `recv` |
| `rbuf` 满且帧仍不完整 | 中止连接（原因 `rbuf full with incomplete frame`） |

注意：`message_handler` 只收到载荷，**收不到 type**。非 PING/PONG 的未定义类型
也会被当作 DATA 交给回调；如果应用需要区分消息类型，应把类型编码进载荷，或修改
实现。`PING` 的回 `PONG` 也受 `max_pending_bytes` 约束：发不出去时会中止连接并
记录原因。

---

## 4. 读路径

```
epoll LT EPOLLIN
  → handle_read(c)
      预算：每次最多 64 KiB / 256 帧
      recv() 直接写进 rbuf 的连续可写段
      预算用完 → schedule_read(c)（挂到 reactor 的待读链表）→ 下一轮继续
  → parse_frames(c)
      完整帧 → 分发（PING/PONG/DATA）
```

几个关键点：

- **`rbuf` 是环形缓冲，但 recv 只使用连续可写段**。环尾只剩 1 字节时就只读 1
  字节，下一次从回绕后的尾部继续；读操作从不跨环尾拆分。写路径的
  `write_span()` / `commit()` 就是为这件事存在的。
- **一次 `handle_read` 有固定预算**，所以“对端发了很多”不会让一条连接独占
  reactor；剩余输入通过 `schedule_read` 在后续轮次继续。
- **`last_recv_ms` 只在 `recv` 拿到字节时更新**，解析帧时不更新。空闲判定看的是
  “对端是否还在发字节”，不是“是否完成了一帧”。一个慢慢滴一个大帧的对端不算
  **空闲**。
- **但帧有独立的装配期限**：从当前帧的第一个字节开始计 `frame_started_at_ms`，后续
  字节不刷新它；超过 `frame_assembly_timeout_ms` 就以“frame assembly timeout”
  中止连接并计入 `frame_assembly_timeouts`。默认 `0` 表示 `5 × idle_timeout_ms`。
  这条期限解决的是“对端一直发字节、但帧永远装配不完”的 slowloris；`idle_timeout_ms`
  对它无效。
- **`frames_recv` 是批量发布的**：每个 reactor 先在本地 `frames_recv_local_` 累加，
  每轮循环（以及退出时）发布一次到共享原子量。空闲时是精确值；有流量时最多滞后
  一轮。
- **handler 默认在 reactor 线程上同步执行**。`payload` 指向 reactor 自己的
  `frame_scratch_`，只在本次回调期间有效；要保留必须自己拷贝。需要把慢工作移出
  reactor 时，用可选的 handler 池（见 4.1）。

### 4.1 慢 handler 卸载（offload）

`config::handler_threads == 0`（默认）时没有池，`offload()` 恒返回 `false`。
设为 N（上限 64）后，`start()` 创建 N 个 worker；message handler 仍在 reactor
上执行，但它可以复制需要的数据，把阻塞部分交给 `offload(conn_id, fn)`：

```cpp
cfg.handler_threads    = 4;
cfg.handler_queue_limit = 8192;   // 所有连接共享的排队上限

server.set_message_handler(
    [&server](uint64_t id, const uint8_t* data, size_t len) {
        std::vector<uint8_t> copy(data, data + len);   // payload 是借用的
        const bool accepted = server.offload(
            id, [srv = &server, id, copy = std::move(copy)] {
                const std::string reply = slow_work(copy);
                (void)srv->send_to(id, reply.data(), reply.size());
            });
        if (!accepted) {
            // 池未启用或队列已满：拒绝、关闭或自行降级处理
        }
    });
```

契约：

- **同一个 `conn_id` 的任务严格串行、先进先出**；不同连接的任务可以并行。
  这保证一条连接的消息不会被两个 worker 同时处理。
- `handler_queue_limit` 是所有连接排队任务总数的上限（默认 8192）。满时
  `offload()` 返回 `false`，任务不会入队；没有单独的统计计数器，由调用方按
  返回值处理。
- 任务在池 worker 上运行，不在 reactor 上。从 worker 调用 `send_to()` 走第 5 节
  的队列路径，仍然线程安全。worker 里同样设置了 `callback_scope`：在 offload
  任务里调用 `stop()` 只发出停止请求。
- 任务抛出的异常会被 worker 捕获并丢弃，不会杀死池线程；这与 inline handler
  的“不要抛”契约一致，但错误不会自动上报。
- `stop()` 在 reactor 停止后设置池停止标志、join worker，并丢弃**尚未开始**的
  任务；已经开始的任务会先完成。因此关机时不保证已入队的 offload 任务一定执行，
  需要可靠完成的工作应在应用层确认。
- **连接关闭不会取消已入队的任务**：它们仍会执行，此时任务里的 `send_to()`
  返回 `not_connected` 或 `closed`。需要随连接取消的工作由应用自己在任务里检查。
- **没有跨路径的额外排序**：handler 在 `offload()` 之后的同步 `send_to` 与
  offload 任务里的 `send_to` 之间按实际提交时刻决定；两个 offload 任务之间才按
  提交顺序。

---

## 5. 写路径与背压

```
应用线程 / handler 调用 send_to(id, data, len)
  ├─ 校验：len <= MAX_FRAME_PAYLOAD、data、is_running()
  ├─ 当前线程是 owner reactor（handler 的常见情况）？
  │    └─ try_send_local:
  │         ├─ 在本 reactor 的 conns_ 查找（无全局锁）
  │         ├─ 未关闭、未 closing、未排队 close_requested
  │         ├─ 预占 pending_bytes
  │         ├─ 直接写 8 字节头 + 载荷到 wbuf
  │         └─ arm_write → ok
  │       （wbuf 没空间或已排队 close 时回退到下面的队列路径）
  └─ 队列路径（非 owner 线程、其它 reactor、或快路径回退）：
       ├─ 查按 conn_id 分片的 conns_ 得到 shared_ptr<connection>
       ├─ 校验连接未关闭/未进入 closing
       ├─ 分配 frame = 8 字节头 + 载荷
       ├─ 预占 pending_bytes（<= max_pending_bytes）
       └─ owner->submit_send(frame)  → reactor 队列 + eventfd
reactor 线程（队列路径）
  ├─ process_pending() 取出任务
  ├─ wbuf.write(frame)
  ├─ arm_write(c)（按需打开 EPOLLOUT）
  └─ handle_write → drain_wbuf_to_socket
       每次最多写 write_budget_bytes
       帧的最后一个字节到达内核 → frames_sent++、bytes_sent += n
```

owner-thread 快路径是常见 echo 用的路径：它不分配 frame/task，不跨队列和
eventfd。`close_requested` 保证“先 close_connection 再 send_to”仍按队列顺序落到
close 之后（因此仍会被丢弃），不会因为快路径提前写 wbuf 而改变语义。

非 owner 队列路径的 submission 屏障和连接表按 `conn_id` 分成 16 个 shard：发送/
关闭只锁自己的 shard，`stop()` 一次性取全部 shard 把在途调用排空。队列用 typed
task（variant）：send_task/close_task/延迟任务内联，新连接任务用 `unique_ptr`（每连接
一次），因此每帧不再分配 `std::function`；分派表由 variant 的 alternative 列表
在编译期生成（`index_sequence`），运行时仍按 variant index 查
function-pointer table。发送仍分配 frame vector，owner 快路径两者都不分配。

`send_result` 的语义：

| 值 | 含义 |
|---|---|
| `ok` | **已接受**（owner 线程直接写 wbuf，或其它线程入队），不表示已送达；关机或断连仍可能丢弃 |
| `not_connected` | 没有这个连接 id |
| `closed` | 服务已停止，或连接已关闭 / 正在关闭 |
| `too_large` | 载荷超过 `MAX_FRAME_PAYLOAD` |
| `overloaded` | 单连接 `pending_bytes` 到顶，或 reactor 队列满 / 分配失败 |
| `invalid_argument` | `data == nullptr && len > 0` |

背压有两层：

1. **单连接 `max_pending_bytes`**（默认 65544）：覆盖“还在队列里的帧 + 已在 wbuf
   里的字节”。这是软上限，超过时 `overloaded` 且 `queue_drops++`。
2. **每 reactor 任务队列上限**：`max(8192, (max_connections / reactors) * 4)`。
   满时提交被拒绝，同样计入 `queue_drops`。

`wbuf_capacity` 默认也是 65544，正好能放下一个最大帧。`start()` 只要求
`wbuf_capacity >= 8`；如果显式调小，最大帧的回复会以 `overloaded`/`too_large`
被拒绝——这是允许的配置，测试套件就利用这一点观察 `overloaded`。

**顺序**：同一线程连续 `send_to` 的帧保持提交顺序；多个线程并发 `send_to` 同一条
连接时，交错顺序未定义。

---

## 6. 关闭协议

关闭分两条路，线上行为不同：

### 6.1 优雅关闭（公开 API 的默认路径）

`close_connection(id)` 把请求投递给 owner reactor，然后：

1. **先服务已经到达的请求**：`drain_input_then_close()` 读取并处理关闭请求到达时
   已经缓冲的输入。它按 `handle_read` 的预算可能分多轮，但只服务“关闭时已经
   在 `rbuf` 里的字节”，`close_input_bytes` 给这个额度封顶；关闭后新到的数据不能
   无限延长关闭。
2. **排空输出**：`park_close()` 置 `closing=true`，把 wbuf 里已有的字节尽量写进
   内核；写不完就打开 `EPOLLOUT`，由 `handle_write` 继续。
3. **发 FIN**：wbuf 空了以后 `shutdown(SHUT_WR)`。关闭**之前**已经排队的应答
   仍会被写出；`closing` 拒绝的是关闭之后的新提交。
4. **等对端 FIN**：继续读，直到 `recv` 返回 0（对端 FIN）或
   `close_linger_ms` 到期。到期仍未收到 FIN 就强制中止。
5. **销毁连接**：`teardown_closed()` → `close(fd)`，然后在安全栈帧上执行延迟的
   `conn_state_handler(false)` 和 `close_handler`。

`close_connection()` 返回 `true` 只表示“请求到达了 reactor 队列”，不表示连接已经
关闭。返回 `false` 表示请求没送到（没有 owner 或队列满），连接仍然活着；
这种情况计入 `close_requests_dropped`。

### 6.2 中止（内部路径）

协议错误、`hup/err`、linger 到期、帧装配超时、PONG 发不出去，以及未排空的
`stop()` 等走
`close_conn(..., graceful=false)`：设置 `SO_LINGER{1,0}`，丢弃待发数据并让内核
发 RST。对端看到的是重置，不是有序 FIN。

### 6.3 延迟回调与 `stop()`

关闭回调不是从关闭连接的栈帧里直接调用的，而是先放进 `pending_closes_`，由
`flush_close_callbacks()` 在连接引用已经释放后执行。这样回调里调用 `stop()` 是
安全的。

`stop()` 的语义：

- 幂等；重复调用或 `start()` 失败后调用都直接返回。
- 停止 accept 线程、各 reactor，清空分片连接表，`port()` 归 0。
- **它对其余连接是中止性的**：仍在关闭中的连接不再等待，由 reactor 的 cleanup
  中止。
- **从回调里调用 `stop()` 只发出停止请求**，不会 join 自己所在线程；控制线程
  必须随后再调用一次 `stop()`（或销毁 server）来完成 join。
- **不要在回调里销毁 `multi_reactor_server`**：析构时 `stop()` 无法 join 当前
  线程，`std::thread` 析构会 `std::terminate()`。最后一个外部 owner 必须在控制
  线程的 `stop()` 返回之后再释放。
- `stop_gracefully(timeout_ms)` 是整机排空版本：先停止 accept（join 后监听套接字
  已关闭），再反复对当前连接调用 `close_connection()`，直到
  `connection_count()==0` 且没有 pending 的新连接，或 `timeout_ms` 到期；最后执行
  正常的 `stop()`。返回 `true` 表示全部排空，`false` 表示超时后剩余连接被
  `stop()` 中止。必须从控制线程调用；从回调里调用只会 `request_stop()` 并返回
  `false`。

---

## 7. 空闲与定时器

| 配置 | 默认 | 作用 |
|---|---|---|
| `tick_ms` | 500 | reactor 检查空闲/linger/帧装配期限的周期 |
| `idle_timeout_ms` | 120000 | 超过这么久没收到任何字节，优雅关闭连接 |
| `frame_assembly_timeout_ms` | `0`（= 5 × `idle_timeout_ms`） | 一个帧从第一个字节到装配完成的上限；超时中止连接 |
| `close_linger_ms` | 5000 | 优雅关闭发出 FIN 后，等对端 FIN 的上限；超时中止 |

- 这些超时都只在 tick 边界检查，实际触发可能比配置值晚最多一个 `tick_ms`。
  `start()` 在 `tick_ms` 大于任一超时时打印警告，但不会拒绝启动。
- 空闲看的是 `last_recv_ms`（最后一次 `recv` 到字节的时间），不是最后一帧完成的
  时间。
- 帧装配期限看的是 `frame_started_at_ms`（当前不完整帧的第一个字节），**不随后续
  字节刷新**。这是它和 `idle_timeout_ms` 的分工：空闲管“对端是否还在发”，装配期限
  管“帧是否在前进”。
- 处于 `closing` 的连接按 `close_started_at_ms` 计算 linger，优先于空闲/装配期限。

---

## 8. 配置

`multi_reactor_server::config`：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `bind_addr` | `"0.0.0.0"` | 监听地址 |
| `port` | `0` | 0 表示内核选临时端口；启动后用 `server.port()` 读回 |
| `backlog` | `1024` | `listen()` backlog，必须 > 0 |
| `num_reactors` | `0` | 0 = `hardware_concurrency()`；上限 64 |
| `max_connections` | `10000` | 准入上限，必须 > 0 |
| `idle_timeout_ms` | `120000` | 空闲关闭阈值，必须 > 0 |
| `frame_assembly_timeout_ms` | `0` | 0 = `5 × idle_timeout_ms`；正数为绝对毫秒；必须 >= 0 |
| `tick_ms` | `500` | 定时器粒度，必须 > 0 |
| `rbuf_capacity` | `MAX_FRAME_SIZE` (65544) | 读缓冲；必须 >= 65544 |
| `wbuf_capacity` | `MAX_FRAME_SIZE` (65544) | 写缓冲；必须 >= 8 |
| `max_pending_bytes` | `MAX_FRAME_SIZE` (65544) | 单连接待发软上限；启动时会被压到 `<= wbuf_capacity` |
| `write_budget_bytes` | `262144` | 单次 drain 最多写多少字节，必须非 0 |
| `close_linger_ms` | `5000` | 优雅关闭等对端 FIN 的上限，必须 > 0 |
| `handler_threads` | `0` | 可选的 offload 池线程数；0 = 不启用；上限 64 |
| `handler_queue_limit` | `8192` | 所有连接共享的 offload 排队上限；启用池时必须 > 0 |

`start()` 在以下情况返回 `false`：

- 服务已经在运行/启动中/停止中；
- `rbuf_capacity < MAX_FRAME_SIZE`；
- `wbuf_capacity < FRAME_HEADER_SIZE`；
- `max_pending_bytes < FRAME_HEADER_SIZE`；
- `max_connections`、`tick_ms`、`idle_timeout_ms`、`close_linger_ms`、`backlog`
  任一 <= 0；
- `frame_assembly_timeout_ms < 0`（0 合法，表示 `5 × idle_timeout_ms`）；
- `write_budget_bytes == 0`；
- `handler_threads < 0`、`> 64`，或 `handler_threads > 0 && handler_queue_limit == 0`；
- 监听 socket、epoll/eventfd、reactor 线程、accept 线程或 handler 池创建失败。

`start()` 成功只表示 accept 线程已经就绪且此刻没有组件失败；`port()` 只有在
`start()` 成功之后、`stop()` 之前才有意义。

---

## 9. 统计

`server.stats()` 返回 `const server_stats&`，字段都是原子量。主要计数器：

| 计数器 | 何时增加 |
|---|---|
| `accepted` | 连接在 reactor 中完成注册（不是 accept 返回时） |
| `rejected` | 超过 `max_connections`，或注册失败 |
| `closed` | 连接完成销毁 |
| `frames_recv` | 解析出一个完整帧；每 reactor 本地累加，每轮发布，空闲精确、繁忙最多滞后一轮 |
| `frames_sent` | 帧的最后一个字节到达内核（不是入队时）；同样本地累加、每轮发布 |
| `bytes_recv` / `bytes_sent` | 实际 recv/send 到内核的字节数；同样本地累加、每轮发布 |
| `queue_drops` | 整帧被拒绝：单连接 `max_pending_bytes` 到顶，或 reactor 队列满 |
| `bytes_dropped` | 连接关闭时仍未发出的字节 |
| `aborted` | 强制断开（RST），不是优雅关闭 |
| `frames_dropped_on_close` | 关闭过程中被丢弃的帧（如 FIN 之后的 PONG） |
| `parse_errors` | magic 错或长度超限 |
| `idle_timeouts` | 空闲超时触发 |
| `frame_assembly_timeouts` | 不完整帧超过 `frame_assembly_timeout_ms`（字节仍在到达） |
| `epoll_ctl_errors` | `epoll_ctl` 失败 |
| `reactor_failures` | reactor 线程异常退出 |
| `accept_errors` | `accept4` 因 fd 耗尽失败（EMFILE/ENFILE） |
| `pending_tasks_total` | 累计入队任务数 |
| `pending_tasks_depth` | 当前队列深度（gauge） |
| `eof_reads` | `recv` 返回 0 的次数（对端 FIN） |
| `close_requests_dropped` | `close_connection` 请求没送到 owner |
| `epipe_writes` | `sendmsg` 返回 EPIPE（预期为 0，用于验证不可达路径） |
| `close_callbacks_dropped` | 关闭回调因分配失败被丢弃（预期为 0） |

四个流量计数器（`frames_recv`、`frames_sent`、`bytes_recv`、`bytes_sent`）都是
“每 reactor 本地累加 + 每轮循环发布一次”：空闲时精确，有流量时最多滞后一轮。
`server_stats` 的热计数器按 64 字节隔离，避免不同 reactor 更新不同计数器时的
false sharing；同一个计数器（例如 `frames_sent`）仍由多个 reactor 更新，但频率
从每帧降到每轮一次。

---

## 10. 使用方法

### 10.1 集成

```bash
# 只需要头文件和两个编译选项
g++ -std=c++17 -O2 -pthread -I<repo>/include your_server.cpp -o your_server
```

单头文件已经包含全部实现；不要把它再拆成 `.cpp`（`inline` 定义就是为此）。

### 10.2 最小可运行服务器

```cpp
#include "tcp-server-mt.h"

#include <cstdio>
#include <string>

int main() {
    industrial::multi_reactor_server::config cfg;
    cfg.bind_addr       = "0.0.0.0";
    cfg.port           = 9000;   // 0 = 内核选端口，启动后读 server.port()
    cfg.num_reactors    = 0;      // 0 = hardware_concurrency()
    cfg.max_connections = 10000;

    industrial::multi_reactor_server server(cfg);

    // 连接建立/关闭。false 回调是延迟执行的，可以安全地调用 stop()。
    server.set_conn_state_handler(
        [](uint64_t id, const std::string& peer, bool connected) {
            std::printf("conn %llu %s %s\n",
                        static_cast<unsigned long long>(id), peer.c_str(),
                        connected ? "opened" : "closed");
        });

    // 收到 DATA（以及任何非 PING/PONG 帧）的载荷。运行在 owner 线程上。
    server.set_message_handler(
        [&server](uint64_t id, const uint8_t* data, size_t len) {
            // 回显。ok 只表示入队，不表示已送达。
            const industrial::send_result r = server.send_to(id, data, len);
            if (r != industrial::send_result::ok) {
                // not_connected / closed / too_large / overloaded /
                // invalid_argument：按业务决定记录或断开。
            }
        });

    // 关闭原因。peer 和 reason 只在回调期间有效。
    server.set_close_handler(
        [](uint64_t id, const std::string& peer, const char* reason) {
            std::printf("close %llu %s: %s\n",
                        static_cast<unsigned long long>(id), peer.c_str(),
                        reason);
        });

    if (!server.start()) {
        std::fprintf(stderr, "start failed (check config)\n");
        return 1;
    }
    std::printf("listening on %u\n", server.port());

    // 真实程序在这里跑自己的事件循环、信号处理或控制线程；
    // 退出时由控制线程调用 server.stop()。
    // wait() 只是每 200ms 轮询 is_running() 的便捷方法。
    server.wait();
    return 0;
}
```

### 10.3 回调契约

- **在 `start()` 之前安装**，运行期间不要替换（没有同步保护）。
- 回调在多个 reactor 线程上并发执行，必须自己保证线程安全。
- **不要抛异常**。异常逃出回调会被 reactor 顶层捕获，`reactor_failures++`，
  `is_running()` 转假。
- **不要让回调阻塞**。回调在 owner reactor 线程上同步执行；一次慢调用（数据库、
  磁盘、锁、同步 RPC）会拖住该 reactor 上的所有连接，表现为其它连接的 PONG/应答
  延迟、空闲超时和尾延迟上升。需要阻塞工作时，用 `offload()`（需
  `handler_threads > 0`）或应用自己的线程池；两者都要保持同一连接的消息顺序。
  `offload()` 已经保证同一 `conn_id` 串行，见 4.1。
- `payload`、`peer`、`reason` 都是借用，只在本次回调期间有效；要保留就拷贝。
- 回调里可以调用 `send_to()`、`close_connection()`、`connection_ids()`、
  `stats()`。
- 回调里可以调用 `stop()`，但它只发出停止请求；控制线程必须随后完成 join。
- **不要在回调里销毁 server**（见 6.3）。

### 10.4 发送与关闭

```cpp
// 任意线程可调用。ok 表示已接受：owner 线程直接写 wbuf，其它线程入队。
industrial::send_result r = server.send_to(id, reply.data(), reply.size());

// 优雅关闭：先送完已排队的回复，再 FIN。
// true = 请求到达 reactor 队列；false = 没送到，连接仍然活着。
bool queued = server.close_connection(id);

// 整机优雅关闭：停止 accept、逐连接 close，最多等 timeout_ms。
// true = 全部排空；false = 超时后剩余连接被 stop() 中止。
bool drained = server.stop_gracefully(3000);

// 可选的慢 handler 卸载；需要 cfg.handler_threads > 0。
// true = 已入队；false = 池未启用或全局排队上限已满。
bool accepted = server.offload(id, [id, &server] {
    // 不在 reactor 线程上；同一连接的 offload 任务串行执行。
    const std::string reply = slow_work();
    (void)server.send_to(id, reply.data(), reply.size());
});
```

### 10.5 客户端侧

服务端只负责帧的收发，客户端可以按第 3 节的协议自己实现。下面是最小发送/接收
参考；测试里的完整实现见 `verify/harness.h` 的 `harness::peer`。

```cpp
#include "tcp-server-mt.h"

#include <cerrno>
#include <cstring>
#include <vector>

bool read_all(int fd, void* buf, size_t len) {
    auto* p = static_cast<uint8_t*>(buf);
    while (len > 0) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n == 0) return false;                 // 对端 FIN
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

bool send_frame(int fd, industrial::frame_type type,
               const void* payload, size_t len) {
    if (len > industrial::MAX_FRAME_PAYLOAD) return false;
    std::vector<uint8_t> frame(industrial::FRAME_HEADER_SIZE + len);
    industrial::encode_frame_into(frame.data(), type, payload, len);
    size_t off = 0;
    while (off < frame.size()) {
        ssize_t n = ::send(fd, frame.data() + off, frame.size() - off, 0);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

bool recv_frame(int fd, industrial::frame_type* type,
               std::vector<uint8_t>* payload) {
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    if (!read_all(fd, hdr, sizeof hdr)) return false;

    const uint16_t magic = (static_cast<uint16_t>(hdr[0]) << 8) | hdr[1];
    if (magic != industrial::FRAME_MAGIC) return false;

    *type = static_cast<industrial::frame_type>(
        (static_cast<uint16_t>(hdr[2]) << 8) | hdr[3]);
    const uint32_t len = (static_cast<uint32_t>(hdr[4]) << 24)
                       | (static_cast<uint32_t>(hdr[5]) << 16)
                       | (static_cast<uint32_t>(hdr[6]) << 8)
                       |  static_cast<uint32_t>(hdr[7]);
    if (len > industrial::MAX_FRAME_PAYLOAD) return false;

    payload->resize(len);
    return read_all(fd, payload->data(), len);
}
```

---

## 11. 构建与验证

### 11.1 运行套件

```bash
bash verify/run.sh                               # plain 配置
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh   # 33 个配置全跑
```

九个套件、每个配置 11 次运行（`close_semantics` 和 `reactor_spread` 各多跑一次
不同 reactor 数），三种配置 `{plain, asan+ubsan, tsan}`，共 33 次。每次运行由
`timeout 180` 兜底，非零退出即失败。

### 11.2 CMake

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure       # 11 个测试
```

`-DTCP_SANITIZE=address,undefined` 或 `-DTCP_SANITIZE=thread` 切换 sanitizer；
`-DTCP_BUILD_BENCH=ON` 打开四个 benchmark。

### 11.3 cpplint

```bash
python3 -m cpplint include/tcp-server-mt.h
```

`CPPLINT.cfg` 固定 80 列，并过滤掉不适用于本仓库的 `legal/copyright`。

### 11.4 套件各自负责什么

| 套件 | 断言 |
|---|---|
| `close_semantics` | 线上可见的关闭行为：FIN vs RST、关闭时未读请求、计数闭合 |
| `idle_semantics` | 空闲按字节而不是按帧判断 |
| `ping_pong` | PING/PONG、关闭途中到达的 PING |
| `send_results` | `send_result` 每个值、`accept_errors`、关闭中发送、offload 串行且不阻塞 reactor |
| `backpressure` | 广播时的队列上限与 `queue_drops` |
| `fault_injection` | reactor/accept 线程异常后的恢复（需要 `INDUSTRIAL_TCP_TESTING`） |
| `reactor_spread` | 轮转分配确实把连接分到不同 reactor |
| `config_budget` | 默认缓冲区预算、`ring_buffer` 就地写 API |
| `harness_selftest` | 测试 harness 自身 |

`verify/evidence/` 下的一次性探针不属于常驻套件，不接入 runner。

---

## 12. 容量与性能（当前机器上的参考值）

- 默认每连接内存约 **129 KB**（VM 实测 129.2 KB/连接，200 连接）。
  默认 `max_connections = 10000` 时约 **1.3 GB** 缓冲。
- 最大帧 **64 KiB + 8 字节头**。
- 在开发 VM 上，4 KB 批量 ingest 约 **2.8 GB/s**，占用 8 核中的约 0.83 核；
  这些数字依赖机器、内核和编译选项，不是承诺。
- 发送 echo（owner-thread 快路径，`bench_send` 多客户端）：1/1 p50 35us /
  p99 63us；2/2 p50 17us / p99 56us；4/4 p50 26us / p99 70us。VM 8 vCPU，
  每客户端 2 万条，10 次中位数；延迟受调度影响，不是承诺。
- 非 owner 队列路径（`-DINDUSTRIAL_TCP_NO_SEND_FASTPATH`）分片锁前后：2/2
  p50 18.4→13.9us、p99 62.1→41.4us、吞吐 92k→131k/s；1/1 与 4/4 在噪声内。
  该路径仍每帧分配 frame buffer；任务本身已在队列内联。
- typed task 与 `std::function` 的 10 次 A/B：1/1 与 2/2 基本持平或略好
  （2/2 130.8k vs 118.6k/s），4/4 在噪声内偏慢（102k vs 110k/s）；主要收益是
  去掉每帧 `std::function` 分配，不是延迟。
- typed task 的分派用 function-pointer table（由 variant 的 alternative 列表在
  编译期生成）；与 switch 的 10 次 A/B 基本持平或略好（4/4 108.4k vs
  104.5k/s，p99 77.6 vs 81.7us）。
- 读路径已经不是瓶颈；剩余差异主要在 kernel 和测试客户端。

---

## 13. 已知边界与注意事项

- **Linux only**：`epoll`、`eventfd`、`accept4`、`SO_LINGER` 的行为都按 Linux
  实现。
- **`send_to` 的 `ok` 不是送达确认**。它只表示帧已被接受（owner 线程写入
  wbuf，或其它线程入队）；关机、断连、关闭中的连接都可能让它最终没有发出。
- **非 owner 发送仍每帧分配 frame buffer**：队列路径的 typed task 不再分配
  `std::function`，但帧本身的 vector 仍分配；owner reactor 上的 handler 快路径
  两者都不分配。性能敏感且从非 owner 线程发送时，优先把发送放在 handler 里。
- **`stop()` 是立即中止**：它不等待在途回复；需要零丢失退出时用
  `stop_gracefully(timeout_ms)`，并给 `close_linger_ms` 留出时间。
- **优雅关闭可能被已有积压延迟**，但只服务关闭时已经缓冲的输入；新到的数据不能
  无限延长关闭，最终由 `close_linger_ms` 兜底。
- **空闲超时看字节，但帧有独立的装配期限**：一个持续发送但永远不发完一帧的
  对端不会因空闲被回收；`frame_assembly_timeout_ms`（默认 5 × `idle_timeout_ms`）
  会中止它并计入 `frame_assembly_timeouts`。`rbuf_capacity` 和 TCP keepalive 仍是
  内存/死连接的第二道约束。如果合法客户端需要更久来装配一个最大帧，应显式调大
  该值。
- **handler 不能抛异常**，也不能在回调里销毁 server。
- **offload 池是可选的**：`handler_threads == 0` 时 `offload()` 恒返回 `false`；
  `handler_queue_limit` 是全局排队上限，满时任务不入队。`stop()` 会丢弃尚未开始的
  offload 任务，任务异常也被吞掉；需要可观测性或可靠完成时由应用层保证。
- **`max_pending_bytes` 是软上限**：并发 `send_to` 与 PONG 预占同时发生时，可能
  短暂看到略旧的值；代价是多拒一帧，不是正确性问题。
- **`wbuf_capacity` 可以小于 `MAX_FRAME_SIZE`**，但此时最大帧的回复会被拒绝。
  默认值故意取在帧上限。
- **没有 TLS、没有客户端库、没有重连/心跳以外的应用层协议**。PING/PONG 是传输层
  保活，不携带应用语义。
- 统计计数器是原子量，但**不是同一时刻的一致快照**；需要精确对账时按增量读，
  或在空闲时读。
