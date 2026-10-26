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
industrial::MultiReactorServer server(cfg);
```

---

## 2. 总体架构

### 2.1 线程与所有权

```
应用线程（任意多个）
    sendTo(id, ...) / closeConnection(id) / connectionIds() / stats()
              │
              │ 查 shared_ptr<Conn>，再把任务投递到连接所属 reactor
              ▼
┌─────────────────────────────────────────────────────────────┐
│ MultiReactorServer                                           │
│   cfg_ / stats_ / conns_（全局视图，shared_mutex 保护）      │
└───────────────┬─────────────────────────────┬───────────────┘
                │ round-robin (pickReactor)   │
                ▼                             ▼
      ┌──────────────────┐          ┌──────────────────┐
      │ SubReactor 0     │   ...    │ SubReactor N-1   │
      │ 自己的线程        │          │ 自己的线程        │
      │ epoll fd         │          │ epoll fd         │
      │ eventfd 唤醒      │          │ eventfd 唤醒      │
      │ conns_ / fdIndex_│          │ conns_ / fdIndex_│
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

- **一个 accept 线程 + N 个 sub-reactor**。`Config::numReactors == 0` 时取
  `std::thread::hardware_concurrency()`，上限 `Config::kMaxReactors = 64`。
- **连接归属固定**：一条连接从注册到销毁只属于一个 reactor。`Conn` 的非原子字段
  只由该 reactor 线程读写；应用线程只通过 `sendTo` / `closeConnection` 触碰
  原子字段，或通过 `connectionIds()` / `stats()` 读全局快照。
- **任务队列 + eventfd**：跨线程提交不是直接操作连接，而是把任务放进目标
  reactor 的队列，再写 eventfd 唤醒它。这保证连接状态始终由 owner 线程修改。
- **owner-thread 快速路径**：handler 本来就在连接所属 reactor 上，调用 `sendTo`
  时可以直接写 wbuf，绕过全局锁、任务队列和 eventfd；只有非 owner 线程、其它
  reactor 的连接，或 wbuf 暂时没空间时才走队列。
- **轮转分配**：`pickReactor()` 用 `nextReactor_` 递增取模，连接均匀落到各
  reactor。`reactor_spread` 套件断言了这一点。

### 2.2 accept 线程

1. `epoll_wait` 监听 `listenFd_` 和 `wakeFd_`。
2. 每轮最多 `accept4` 64 个连接，避免新连接洪峰饿死其它 fd。
3. 设置 `TCP_NODELAY`、`SO_KEEPALIVE`（`TCP_KEEPIDLE=30s`、
   `TCP_KEEPINTVL=10s`、`TCP_KEEPCNT=3`）。
4. 取对端地址字符串，分配递增的连接 id。
5. 准入：`maxConnections` 用 `Admission` 的 RAII 计数控制；超限直接 `rejected++`
   并关闭新 fd。
6. 轮转选 reactor，把 fd 所有权交给它；注册成功后才 `accepted++`，失败则
   `rejected++`。

`accepted` 统计的是“已经注册进 reactor 的连接”，不是“accept 返回的 fd”；
`maxConnections` 的准入计数则覆盖从 accept 到连接销毁的整个生命周期。

### 2.3 reactor 主循环

每个 reactor 线程大致做这些事，顺序固定：

```
while (running && server->isRunning()) {
    处理本 reactor 的待读连接（最多 64 条，每条 handleRead 有预算）
    发布 framesRecvLocal_ → stats.framesRecv
    epoll_wait(..., readHead_ ? 0 : tickMs)
    处理 EPOLLIN / EPOLLOUT / EPOLLHUP|EPOLLERR
    flushCloseCallbacks()     // 延迟的关闭回调
    processPending()          // 任务队列
    flushCloseCallbacks()
    到 tick 点则 doTick()     // 空闲与 linger 扫描
}
```

`readHead_` 非空时 `epoll_wait` 超时为 0：本 reactor 还有欠读的连接，处理完再等。
`doTick()` 只在 `tickMs` 到达时运行；空闲超时和关闭 linger 都按 tick 粒度检查。

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
| `DATA` 及其它非 PING/PONG 类型 | 把载荷交给 `MessageHandler(id, payload, len)` |
| magic 错 | `parseErrors++`，中止连接（RST） |
| `length > MAX_FRAME_PAYLOAD` | `parseErrors++`，中止连接（RST） |
| 帧不完整 | 留在 `rbuf`，等下一次 `recv` |
| `rbuf` 满且帧仍不完整 | 中止连接（原因 `rbuf full with incomplete frame`） |

注意：`MessageHandler` 只收到载荷，**收不到 type**。非 PING/PONG 的未定义类型
也会被当作 DATA 交给回调；如果应用需要区分消息类型，应把类型编码进载荷，或修改
实现。`PING` 的回 `PONG` 也受 `maxPendingBytes` 约束：发不出去时会中止连接并
记录原因。

---

## 4. 读路径

```
epoll LT EPOLLIN
  → handleRead(c)
      预算：每次最多 64 KiB / 256 帧
      recv() 直接写进 rbuf 的连续可写段
      预算用完 → scheduleRead(c)（挂到 reactor 的待读链表）→ 下一轮继续
  → parseFrames(c)
      完整帧 → 分发（PING/PONG/DATA）
```

几个关键点：

- **`rbuf` 是环形缓冲，但 recv 只使用连续可写段**。环尾只剩 1 字节时就只读 1
  字节，下一次从回绕后的尾部继续；读操作从不跨环尾拆分。写路径的
  `writeSpan()` / `commit()` 就是为这件事存在的。
- **一次 `handleRead` 有固定预算**，所以“对端发了很多”不会让一条连接独占
  reactor；剩余输入通过 `scheduleRead` 在后续轮次继续。
- **`lastRecvMs` 只在 `recv` 拿到字节时更新**，解析帧时不更新。空闲判定看的是
  “对端是否还在发字节”，不是“是否完成了一帧”。一个慢慢滴一个大帧的对端不算
  **空闲**。
- **但帧有独立的装配期限**：从当前帧的第一个字节开始计 `frameStartedAtMs`，后续
  字节不刷新它；超过 `frameAssemblyTimeoutMs` 就以“frame assembly timeout”
  中止连接并计入 `frameAssemblyTimeouts`。默认 `0` 表示 `5 × idleTimeoutMs`。
  这条期限解决的是“对端一直发字节、但帧永远装配不完”的 slowloris；`idleTimeoutMs`
  对它无效。
- **`framesRecv` 是批量发布的**：每个 reactor 先在本地 `framesRecvLocal_` 累加，
  每轮循环（以及退出时）发布一次到共享原子量。空闲时是精确值；有流量时最多滞后
  一轮。
- **handler 在 reactor 线程上同步执行**。`payload` 指向 reactor 自己的
  `frameScratch_`，只在本次回调期间有效；要保留必须自己拷贝。

---

## 5. 写路径与背压

```
应用线程 / handler 调用 sendTo(id, data, len)
  ├─ 校验：len <= MAX_FRAME_PAYLOAD、data、isRunning()
  ├─ 当前线程是 owner reactor（handler 的常见情况）？
  │    └─ trySendLocal:
  │         ├─ 在本 reactor 的 conns_ 查找（无全局锁）
  │         ├─ 未关闭、未 closing、未排队 closeRequested
  │         ├─ 预占 pendingBytes
  │         ├─ 直接写 8 字节头 + 载荷到 wbuf
  │         └─ armWrite → Ok
  │       （wbuf 没空间或已排队 close 时回退到下面的队列路径）
  └─ 队列路径（非 owner 线程、其它 reactor、或快路径回退）：
       ├─ 查按 connId 分片的 conns_ 得到 shared_ptr<Conn>
       ├─ 校验连接未关闭/未进入 closing
       ├─ 分配 frame = 8 字节头 + 载荷
       ├─ 预占 pendingBytes（<= maxPendingBytes）
       └─ owner->submitSend(frame)  → reactor 队列 + eventfd
reactor 线程（队列路径）
  ├─ processPending() 取出任务
  ├─ wbuf.write(frame)
  ├─ armWrite(c)（按需打开 EPOLLOUT）
  └─ handleWrite → drainWbufToSocket
       每次最多写 writeBudgetBytes
       帧的最后一个字节到达内核 → framesSent++、bytesSent += n
```

owner-thread 快路径是常见 echo 用的路径：它不分配 frame/task，不跨队列和
eventfd。`closeRequested` 保证“先 closeConnection 再 sendTo”仍按队列顺序落到
close 之后（因此仍会被丢弃），不会因为快路径提前写 wbuf 而改变语义。

非 owner 队列路径的 submission 屏障和连接表按 `connId` 分成 16 个 shard：发送/
关闭只锁自己的 shard，`stop()` 一次性取全部 shard 把在途调用排空。队列用 typed
task（variant）：Send/Close/延迟任务内联，新连接任务用 `unique_ptr`（每连接
一次），因此每帧不再分配 `std::function`；分派按 variant index 查
function-pointer table。发送仍分配 frame vector，owner 快路径两者都不分配。

`SendResult` 的语义：

| 值 | 含义 |
|---|---|
| `Ok` | **已接受**（owner 线程直接写 wbuf，或其它线程入队），不表示已送达；关机或断连仍可能丢弃 |
| `NotConnected` | 没有这个连接 id |
| `Closed` | 服务已停止，或连接已关闭 / 正在关闭 |
| `TooLarge` | 载荷超过 `MAX_FRAME_PAYLOAD` |
| `Overloaded` | 单连接 `pendingBytes` 到顶，或 reactor 队列满 / 分配失败 |
| `InvalidArgument` | `data == nullptr && len > 0` |

背压有两层：

1. **单连接 `maxPendingBytes`**（默认 65544）：覆盖“还在队列里的帧 + 已在 wbuf
   里的字节”。这是软上限，超过时 `Overloaded` 且 `queueDrops++`。
2. **每 reactor 任务队列上限**：`max(8192, (maxConnections / reactors) * 4)`。
   满时提交被拒绝，同样计入 `queueDrops`。

`wbufCapacity` 默认也是 65544，正好能放下一个最大帧。`start()` 只要求
`wbufCapacity >= 8`；如果显式调小，最大帧的回复会以 `Overloaded`/`TooLarge`
被拒绝——这是允许的配置，测试套件就利用这一点观察 `Overloaded`。

**顺序**：同一线程连续 `sendTo` 的帧保持提交顺序；多个线程并发 `sendTo` 同一条
连接时，交错顺序未定义。

---

## 6. 关闭协议

关闭分两条路，线上行为不同：

### 6.1 优雅关闭（公开 API 的默认路径）

`closeConnection(id)` 把请求投递给 owner reactor，然后：

1. **先服务已经到达的请求**：`drainInputThenClose()` 读取并处理关闭请求到达时
   已经缓冲的输入。它按 `handleRead` 的预算可能分多轮，但只服务“关闭时已经
   在 `rbuf` 里的字节”，`closeInputBytes` 给这个额度封顶；关闭后新到的数据不能
   无限延长关闭。
2. **排空输出**：`parkClose()` 置 `closing=true`，把 wbuf 里已有的字节尽量写进
   内核；写不完就打开 `EPOLLOUT`，由 `handleWrite` 继续。
3. **发 FIN**：wbuf 空了以后 `shutdown(SHUT_WR)`。关闭**之前**已经排队的应答
   仍会被写出；`closing` 拒绝的是关闭之后的新提交。
4. **等对端 FIN**：继续读，直到 `recv` 返回 0（对端 FIN）或
   `closeLingerMs` 到期。到期仍未收到 FIN 就强制中止。
5. **销毁连接**：`teardownClosed()` → `close(fd)`，然后在安全栈帧上执行延迟的
   `ConnStateHandler(false)` 和 `CloseHandler`。

`closeConnection()` 返回 `true` 只表示“请求到达了 reactor 队列”，不表示连接已经
关闭。返回 `false` 表示请求没送到（没有 owner 或队列满），连接仍然活着；
这种情况计入 `closeRequestsDropped`。

### 6.2 中止（内部路径）

协议错误、`hup/err`、linger 到期、帧装配超时、PONG 发不出去，以及未排空的
`stop()` 等走
`closeConn(..., graceful=false)`：设置 `SO_LINGER{1,0}`，丢弃待发数据并让内核
发 RST。对端看到的是重置，不是有序 FIN。

### 6.3 延迟回调与 `stop()`

关闭回调不是从关闭连接的栈帧里直接调用的，而是先放进 `pendingCloses_`，由
`flushCloseCallbacks()` 在连接引用已经释放后执行。这样回调里调用 `stop()` 是
安全的。

`stop()` 的语义：

- 幂等；重复调用或 `start()` 失败后调用都直接返回。
- 停止 accept 线程、各 reactor，清空分片连接表，`port()` 归 0。
- **它对其余连接是中止性的**：仍在关闭中的连接不再等待，由 reactor 的 cleanup
  中止。
- **从回调里调用 `stop()` 只发出停止请求**，不会 join 自己所在线程；控制线程
  必须随后再调用一次 `stop()`（或销毁 server）来完成 join。
- **不要在回调里销毁 `MultiReactorServer`**：析构时 `stop()` 无法 join 当前
  线程，`std::thread` 析构会 `std::terminate()`。最后一个外部 owner 必须在控制
  线程的 `stop()` 返回之后再释放。
- `stopGracefully(timeoutMs)` 是整机排空版本：先停止 accept（join 后监听套接字
  已关闭），再反复对当前连接调用 `closeConnection()`，直到
  `connectionCount()==0` 且没有 pending 的新连接，或 `timeoutMs` 到期；最后执行
  正常的 `stop()`。返回 `true` 表示全部排空，`false` 表示超时后剩余连接被
  `stop()` 中止。必须从控制线程调用；从回调里调用只会 `requestStop()` 并返回
  `false`。

---

## 7. 空闲与定时器

| 配置 | 默认 | 作用 |
|---|---|---|
| `tickMs` | 500 | reactor 检查空闲/linger/帧装配期限的周期 |
| `idleTimeoutMs` | 120000 | 超过这么久没收到任何字节，优雅关闭连接 |
| `frameAssemblyTimeoutMs` | `0`（= 5 × `idleTimeoutMs`） | 一个帧从第一个字节到装配完成的上限；超时中止连接 |
| `closeLingerMs` | 5000 | 优雅关闭发出 FIN 后，等对端 FIN 的上限；超时中止 |

- 这些超时都只在 tick 边界检查，实际触发可能比配置值晚最多一个 `tickMs`。
  `start()` 在 `tickMs` 大于任一超时时打印警告，但不会拒绝启动。
- 空闲看的是 `lastRecvMs`（最后一次 `recv` 到字节的时间），不是最后一帧完成的
  时间。
- 帧装配期限看的是 `frameStartedAtMs`（当前不完整帧的第一个字节），**不随后续
  字节刷新**。这是它和 `idleTimeoutMs` 的分工：空闲管“对端是否还在发”，装配期限
  管“帧是否在前进”。
- 处于 `closing` 的连接按 `closeStartedAtMs` 计算 linger，优先于空闲/装配期限。

---

## 8. 配置

`MultiReactorServer::Config`：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `bindAddr` | `"0.0.0.0"` | 监听地址 |
| `port` | `0` | 0 表示内核选临时端口；启动后用 `server.port()` 读回 |
| `backlog` | `1024` | `listen()` backlog，必须 > 0 |
| `numReactors` | `0` | 0 = `hardware_concurrency()`；上限 64 |
| `maxConnections` | `10000` | 准入上限，必须 > 0 |
| `idleTimeoutMs` | `120000` | 空闲关闭阈值，必须 > 0 |
| `frameAssemblyTimeoutMs` | `0` | 0 = `5 × idleTimeoutMs`；正数为绝对毫秒；必须 >= 0 |
| `tickMs` | `500` | 定时器粒度，必须 > 0 |
| `rbufCapacity` | `MAX_FRAME_SIZE` (65544) | 读缓冲；必须 >= 65544 |
| `wbufCapacity` | `MAX_FRAME_SIZE` (65544) | 写缓冲；必须 >= 8 |
| `maxPendingBytes` | `MAX_FRAME_SIZE` (65544) | 单连接待发软上限；启动时会被压到 `<= wbufCapacity` |
| `writeBudgetBytes` | `262144` | 单次 drain 最多写多少字节，必须非 0 |
| `closeLingerMs` | `5000` | 优雅关闭等对端 FIN 的上限，必须 > 0 |

`start()` 在以下情况返回 `false`：

- 服务已经在运行/启动中/停止中；
- `rbufCapacity < MAX_FRAME_SIZE`；
- `wbufCapacity < FRAME_HEADER_SIZE`；
- `maxPendingBytes < FRAME_HEADER_SIZE`；
- `maxConnections`、`tickMs`、`idleTimeoutMs`、`closeLingerMs`、`backlog`
  任一 <= 0；
- `frameAssemblyTimeoutMs < 0`（0 合法，表示 `5 × idleTimeoutMs`）；
- `writeBudgetBytes == 0`；
- 监听 socket、epoll/eventfd、reactor 线程或 accept 线程创建失败。

`start()` 成功只表示 accept 线程已经就绪且此刻没有组件失败；`port()` 只有在
`start()` 成功之后、`stop()` 之前才有意义。

---

## 9. 统计

`server.stats()` 返回 `const ServerStats&`，字段都是原子量。主要计数器：

| 计数器 | 何时增加 |
|---|---|
| `accepted` | 连接在 reactor 中完成注册（不是 accept 返回时） |
| `rejected` | 超过 `maxConnections`，或注册失败 |
| `closed` | 连接完成销毁 |
| `framesRecv` | 解析出一个完整帧；每 reactor 本地累加，每轮发布，空闲精确、繁忙最多滞后一轮 |
| `framesSent` | 帧的最后一个字节到达内核（不是入队时）；同样本地累加、每轮发布 |
| `bytesRecv` / `bytesSent` | 实际 recv/send 到内核的字节数；同样本地累加、每轮发布 |
| `queueDrops` | 整帧被拒绝：单连接 `maxPendingBytes` 到顶，或 reactor 队列满 |
| `bytesDropped` | 连接关闭时仍未发出的字节 |
| `aborted` | 强制断开（RST），不是优雅关闭 |
| `framesDroppedOnClose` | 关闭过程中被丢弃的帧（如 FIN 之后的 PONG） |
| `parseErrors` | magic 错或长度超限 |
| `idleTimeouts` | 空闲超时触发 |
| `frameAssemblyTimeouts` | 不完整帧超过 `frameAssemblyTimeoutMs`（字节仍在到达） |
| `epollCtlErrors` | `epoll_ctl` 失败 |
| `reactorFailures` | reactor 线程异常退出 |
| `acceptErrors` | `accept4` 因 fd 耗尽失败（EMFILE/ENFILE） |
| `pendingTasksTotal` | 累计入队任务数 |
| `pendingTasksDepth` | 当前队列深度（gauge） |
| `eofReads` | `recv` 返回 0 的次数（对端 FIN） |
| `closeRequestsDropped` | `closeConnection` 请求没送到 owner |
| `epipeWrites` | `sendmsg` 返回 EPIPE（预期为 0，用于验证不可达路径） |
| `closeCallbacksDropped` | 关闭回调因分配失败被丢弃（预期为 0） |

四个流量计数器（`framesRecv`、`framesSent`、`bytesRecv`、`bytesSent`）都是
“每 reactor 本地累加 + 每轮循环发布一次”：空闲时精确，有流量时最多滞后一轮。
`ServerStats` 的热计数器按 64 字节隔离，避免不同 reactor 更新不同计数器时的
false sharing；同一个计数器（例如 `framesSent`）仍由多个 reactor 更新，但频率
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
    industrial::MultiReactorServer::Config cfg;
    cfg.bindAddr       = "0.0.0.0";
    cfg.port           = 9000;   // 0 = 内核选端口，启动后读 server.port()
    cfg.numReactors    = 0;      // 0 = hardware_concurrency()
    cfg.maxConnections = 10000;

    industrial::MultiReactorServer server(cfg);

    // 连接建立/关闭。false 回调是延迟执行的，可以安全地调用 stop()。
    server.setConnStateHandler(
        [](uint64_t id, const std::string& peer, bool connected) {
            std::printf("conn %llu %s %s\n",
                        static_cast<unsigned long long>(id), peer.c_str(),
                        connected ? "opened" : "closed");
        });

    // 收到 DATA（以及任何非 PING/PONG 帧）的载荷。运行在 owner 线程上。
    server.setMessageHandler(
        [&server](uint64_t id, const uint8_t* data, size_t len) {
            // 回显。Ok 只表示入队，不表示已送达。
            const industrial::SendResult r = server.sendTo(id, data, len);
            if (r != industrial::SendResult::Ok) {
                // NotConnected / Closed / TooLarge / Overloaded /
                // InvalidArgument：按业务决定记录或断开。
            }
        });

    // 关闭原因。peer 和 reason 只在回调期间有效。
    server.setCloseHandler(
        [](uint64_t id, const std::string& peer, const char* reason) {
            std::printf("close %llu %s: %s\n",
                        static_cast<unsigned long long>(id), peer.c_str(),
                        reason);
        });

    if (!server.start()) {
        std::fprintf(stderr, "start failed (check Config)\n");
        return 1;
    }
    std::printf("listening on %u\n", server.port());

    // 真实程序在这里跑自己的事件循环、信号处理或控制线程；
    // 退出时由控制线程调用 server.stop()。
    // wait() 只是每 200ms 轮询 isRunning() 的便捷方法。
    server.wait();
    return 0;
}
```

### 10.3 回调契约

- **在 `start()` 之前安装**，运行期间不要替换（没有同步保护）。
- 回调在多个 reactor 线程上并发执行，必须自己保证线程安全。
- **不要抛异常**。异常逃出回调会被 reactor 顶层捕获，`reactorFailures++`，
  `isRunning()` 转假。
- **不要在回调里阻塞**。回调在 owner reactor 线程上同步执行；一次慢调用（数据库、
  磁盘、锁、同步 RPC）会拖住该 reactor 上的所有连接，表现为其它连接的 PONG/应答
  延迟、空闲超时和尾延迟上升。需要阻塞工作时，交给应用自己的线程池，并保持同一
  连接的消息顺序。
- `payload`、`peer`、`reason` 都是借用，只在本次回调期间有效；要保留就拷贝。
- 回调里可以调用 `sendTo()`、`closeConnection()`、`connectionIds()`、
  `stats()`。
- 回调里可以调用 `stop()`，但它只发出停止请求；控制线程必须随后完成 join。
- **不要在回调里销毁 server**（见 6.3）。

### 10.4 发送与关闭

```cpp
// 任意线程可调用。Ok 表示已接受：owner 线程直接写 wbuf，其它线程入队。
industrial::SendResult r = server.sendTo(id, reply.data(), reply.size());

// 优雅关闭：先送完已排队的回复，再 FIN。
// true = 请求到达 reactor 队列；false = 没送到，连接仍然活着。
bool queued = server.closeConnection(id);

// 整机优雅关闭：停止 accept、逐连接 close，最多等 timeoutMs。
// true = 全部排空；false = 超时后剩余连接被 stop() 中止。
bool drained = server.stopGracefully(3000);
```

### 10.5 客户端侧

服务端只负责帧的收发，客户端可以按第 3 节的协议自己实现。下面是最小发送/接收
参考；测试里的完整实现见 `verify/harness.h` 的 `harness::Peer`。

```cpp
#include "tcp-server-mt.h"

#include <cerrno>
#include <cstring>
#include <vector>

bool readAll(int fd, void* buf, size_t len) {
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

bool sendFrame(int fd, industrial::FrameType type,
               const void* payload, size_t len) {
    if (len > industrial::MAX_FRAME_PAYLOAD) return false;
    std::vector<uint8_t> frame(industrial::FRAME_HEADER_SIZE + len);
    industrial::encodeFrameInto(frame.data(), type, payload, len);
    size_t off = 0;
    while (off < frame.size()) {
        ssize_t n = ::send(fd, frame.data() + off, frame.size() - off, 0);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

bool recvFrame(int fd, industrial::FrameType* type,
               std::vector<uint8_t>* payload) {
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    if (!readAll(fd, hdr, sizeof hdr)) return false;

    const uint16_t magic = (static_cast<uint16_t>(hdr[0]) << 8) | hdr[1];
    if (magic != industrial::FRAME_MAGIC) return false;

    *type = static_cast<industrial::FrameType>(
        (static_cast<uint16_t>(hdr[2]) << 8) | hdr[3]);
    const uint32_t len = (static_cast<uint32_t>(hdr[4]) << 24)
                       | (static_cast<uint32_t>(hdr[5]) << 16)
                       | (static_cast<uint32_t>(hdr[6]) << 8)
                       |  static_cast<uint32_t>(hdr[7]);
    if (len > industrial::MAX_FRAME_PAYLOAD) return false;

    payload->resize(len);
    return readAll(fd, payload->data(), len);
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
| `send_results` | `SendResult` 每个值、`acceptErrors`、关闭中发送 |
| `backpressure` | 广播时的队列上限与 `queueDrops` |
| `fault_injection` | reactor/accept 线程异常后的恢复（需要 `INDUSTRIAL_TCP_TESTING`） |
| `reactor_spread` | 轮转分配确实把连接分到不同 reactor |
| `config_budget` | 默认缓冲区预算、`RingBuffer` 就地写 API |
| `harness_selftest` | 测试 harness 自身 |

`verify/evidence/` 下的一次性探针不属于常驻套件，不接入 runner。

---

## 12. 容量与性能（当前机器上的参考值）

- 默认每连接内存约 **129 KB**（VM 实测 129.2 KB/连接，200 连接）。
  默认 `maxConnections = 10000` 时约 **1.3 GB** 缓冲。
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
- typed task 的分派用 function-pointer table；与 switch 的 10 次 A/B 基本持平或
  略好（4/4 108.4k vs 104.5k/s，p99 77.6 vs 81.7us）。
- 读路径已经不是瓶颈；剩余差异主要在 kernel 和测试客户端。

---

## 13. 已知边界与注意事项

- **Linux only**：`epoll`、`eventfd`、`accept4`、`SO_LINGER` 的行为都按 Linux
  实现。
- **`sendTo` 的 `Ok` 不是送达确认**。它只表示帧已被接受（owner 线程写入
  wbuf，或其它线程入队）；关机、断连、关闭中的连接都可能让它最终没有发出。
- **非 owner 发送仍每帧分配 frame buffer**：队列路径的 typed task 不再分配
  `std::function`，但帧本身的 vector 仍分配；owner reactor 上的 handler 快路径
  两者都不分配。性能敏感且从非 owner 线程发送时，优先把发送放在 handler 里。
- **`stop()` 是立即中止**：它不等待在途回复；需要零丢失退出时用
  `stopGracefully(timeoutMs)`，并给 `closeLingerMs` 留出时间。
- **优雅关闭可能被已有积压延迟**，但只服务关闭时已经缓冲的输入；新到的数据不能
  无限延长关闭，最终由 `closeLingerMs` 兜底。
- **空闲超时看字节，但帧有独立的装配期限**：一个持续发送但永远不发完一帧的
  对端不会因空闲被回收；`frameAssemblyTimeoutMs`（默认 5 × `idleTimeoutMs`）
  会中止它并计入 `frameAssemblyTimeouts`。`rbufCapacity` 和 TCP keepalive 仍是
  内存/死连接的第二道约束。如果合法客户端需要更久来装配一个最大帧，应显式调大
  该值。
- **handler 不能抛异常**，也不能在回调里销毁 server。
- **`maxPendingBytes` 是软上限**：并发 `sendTo` 与 PONG 预占同时发生时，可能
  短暂看到略旧的值；代价是多拒一帧，不是正确性问题。
- **`wbufCapacity` 可以小于 `MAX_FRAME_SIZE`**，但此时最大帧的回复会被拒绝。
  默认值故意取在帧上限。
- **没有 TLS、没有客户端库、没有重连/心跳以外的应用层协议**。PING/PONG 是传输层
  保活，不携带应用语义。
- 统计计数器是原子量，但**不是同一时刻的一致快照**；需要精确对账时按增量读，
  或在空闲时读。
