# CONTEXT.md

这个仓库的承重词。它们只活在注释和 `docs/review/` 的散文里，新读者（或 agent）要理解
`deferredRound` 的两种取值，得读 `CloseRound` 上方那段注释，再翻到
`docs/review/CODE-REVIEW-2026-10-18-round7.md` 才知道为什么（那篇第 10 行就是
「一个 bool 被两个主人共用，含义相反」）。

这里给每个词一句定义、一个反例、一个出处。**反例比定义有用**——这些词之所以需要定义，
正是因为它们各自被误解过一次，而且每次误解都成了一个真实缺陷。

---

## 项目是什么

一个 Linux TCP 服务器，压在一个头文件里：C++17、level-triggered epoll、一个 accept 线程
加 N 个 sub-reactor。没有 `.cpp`，没有第三方依赖。产品承诺是「复制
`include/tcp-server-mt.h` 到你的项目里」。

这意味着：**可读性和可维护性就是这个库的产品**，不是附带指标。

---

## 承重词

### phase（阶段）

由 `isClosed` / `closing` / `finSent` 三个标志**派生**的四个阶段：
`Open` → `Closing` → `FinSent` → `Torn`。`Conn::phase()` 是唯一的计算处，`decide()` 是唯一
的决策处。

> **反例**：把 phase 当成一个字段存起来。那会变成第四个必须和另外三个保持同步的标志，
> 而「必须保持同步的字段」正是这套机制要消灭的东西。

出处：`ClosePhase`、`decide()`、`docs/architecture/r3-baseline.md` 第 3 节。

### park（停放）

优雅关闭的一种中间态：连接还在、fd 还在、读还在跑，但**拒绝新的提交**，等 wbuf 排空
（`Park`）或等对端的 FIN。

> **反例**：park ≠ 关闭。park 的 fd 仍然注册在 epoll 里，仍然能被 sendTo 找到，
> 只是 `submitSend` 会拒绝新帧并计入 `queueDrops` + `bytesDropped`。

出处：`parkClose()`、`CloseAction::Park`。

### defer 一轮（deferredRound）

关闭过程中"再等一个队列批次"。两种取值，**必须分开**：

- `Close` —— `drainInputThenClose()` 欠的，轮次结束于 park。
- `PeerClosed` —— `finishPeerClosed()` 欠的，轮次结束于它自己再判一次。

> **反例**：用一个 bool 表示"轮次已用"。这就是 R8 之前的写法，代价是对端丢了最后一个
> 回复——被读成"轮次已用，现在 park"，而实际是"轮次仍被欠着"，于是提前一轮 park，
> wbuf 还是空的，FIN 立刻发出，压过了那些还没来得及跑完的回复。

出处：`CloseRound`、`Conn::close.round`。

### linger（半关宽限期）

我们发出 FIN 之后，还愿意等对端 FIN 多久（`closeLingerMs`）。超时就 abort。

> **反例**：linger 不是"等对端回话"。对端已经把话说完才发的 FIN；这段等的是它把自己
> 的 FIN 也发过来，而不是内容。

出处：`doTick()` 的 linger 分支、`Config::closeLingerMs`。

### graceful / abort

两条出口，区别在线上可见：

- `graceful` —— 先把欠对端的字节投递完，再 `shutdown(SHUT_WR)`，等对端 FIN 才 `close()`。
- `abort` —— 丢弃待发数据，`SO_LINGER{on,0}` 请求内核发 RST。

> **反例**：以为 `close(fd)` 就是优雅关闭。对端还有未读数据时，`close()` 会发 RST 并丢弃
> 排队的输出——这正是这套机制存在的全部理由。

出处：`unregisterConn()`、`docs/review/` 里 P0-5 与 case C/E/F。

### admission（准入票据）

每个连接一个 RAII 票据，`admitted_` 是连接数上限的实际门闸。任何失败/丢弃路径都靠同一个
析构函数归还。

> **反例**：拿 `conns_.size()` 当连接数上限。那是索引大小，不是准入计数——两者在注册
> 失败、拒绝连接这些路径上会短暂不一致，而 `admitted_` 才是 accept 循环读的那个。

出处：`Admission`、`acceptBatch()`。

### credit / pendingBytes（输出配额）

已承诺但还没交给内核的字节数：既包括还在队列里的 send 任务，也包括已经躺在 wbuf 里的。
一个数字盖住两段，因为单独任何一段都不能说明对端是否跟得上。

> **反例**：把 `wbuf.empty()` 当成"没有欠账"。wbuf 空的时候，队列里仍可能有已预约的帧——
> `handleWrite` 里那段长注释就是在解释为什么这里不能加 `pendingBytes` 守卫。

出处：`Conn::out.pendingBytes`。

### drain（排空）

把 wbuf 推进内核。结果有四种（`DrainOutcome`），**不能混**：

- `Drained` —— wbuf 空了，该干嘛干嘛。
- `Stalled` —— 预算用完或 EAGAIN，EPOLLOUT 会接上。
- `Dead` —— EPIPE，这个方向永远发不出去了。
- `Error` —— 其它，致命。

> **反例**：曾经用 `0 / 1 / -1` 表示，`Drained` 和 `EPIPE` 共用了 `0`。把它们当同一件事，
> 等于把连接的去留交给调用点的 `wbuf.empty()`——一个没有任何调用点说出口的事实。

出处：`DrainOutcome`、`drainWbufToSocket()`。

### peerFin（对端半关）

`recv()` 返回 0。对端把写方向关了，但它可能还在读。

> **反例**：收到 FIN 就 close。那样我们欠它的回复会变成 RST。正确做法是先把欠的投递完，
> 再 FIN，再等它自己关。

出处：`Conn::close.peerFinSeen`、`finishPeerClosed()`。

---

## 读代码的入口

| 想改什么 | 先读 |
|---|---|
| 关闭行为 | `docs/architecture/r3-baseline.md` 第 5 节（五张决策表） |
| 关闭为什么这样 | `docs/review/README.md` 的八轮索引，按 P0/P1 找现场 |
| 性能数字 | `verify/bench.sh` + 本仓库 commit message 里的前后对比 |
| 公开 API | `verify/evidence/probe_surface_shrank.sh`（它定义了什么不该被看到） |
| 环形缓冲区契约 | `verify/ring_buffer.cpp`（比注释更准） |

---

## 一条纪律

这个仓库不接受"看起来更对"。每一处改动要么有一个能失败的测试，要么有一个能对比的数字。
`docs/review/README.md` 末尾那句话是这个仓库最重要的一条约定：

> Nothing was written down as a defect without runtime evidence.

反过来也成立：**没有运行时证据的"修复"同样不算数。**
