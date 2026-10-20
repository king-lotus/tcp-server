# 架构分析：`include/tcp-server-mt.h`

分析对象：`include/tcp-server-mt.h`（2989 行，1702 代码行）
方法：静态结构分析 + 度量 + 既有 8 轮评审记录的交叉比对
性质：**架构层**评估，不重复 `docs/review/` 里的缺陷清单。本文只回答"结构哪里会持续产生缺陷"，不回答"第 N 行有 bug"。

---

## 0. 结论

| 维度 | 判断 |
|---|---|
| 分层 | 5 层存在，但 L3/L4 边界是名义上的；`Conn` 与 `SubReactor` 共享同一套状态机寄存器 |
| 依赖方向 | L4 ↔ L5 **双向循环**，靠 `friend class SubReactor` 打开私有面 |
| 单一职责 | 3 处明显越界：`start()`（8 职责）、`acceptLoop()`（7 职责）、`Conn`（4 类状态混装） |
| 状态表达 | 关闭协议 = 7 个字段 + 5 个转移函数 + 26 个入口，**无单一转移函数** |
| 规格载体 | 39.4% 的注释承担了不变量说明；`handleWrite` 86 行跨度里只有 23 行代码 |
| 分发契约 | 单文件是产品承诺，不能拆；但公开符号面可以收窄 70% |

核心判断：**这个文件的复杂度不在算法，在状态**。8 轮评审的主要落点（case E / case F / P0-5 / R7-1 / R8-1）全部落在关闭协议，而关闭协议的复杂度来自"同一事实被 7 个字段分别记录"。这是结构问题，不是实现问题。

---

## 1. 度量基线

| 指标 | 值 |
|---|---|
| 总行数 / 代码行 / 注释行 / 空行 | 2989 / 1702 / 1106 / 181 |
| 注释占非空行比 | **39.4%** |
| 函数数 | 41 |
| 最长函数 | `start()` 169 行（115 代码行） |
| 次长 | `acceptLoop()` 163 行、`parseFrames()` 114 行、`drainInputThenClose()` 113 行 |
| `Conn` 字段数 | 23 |
| `SubReactor` 方法数 | 32 |
| `ServerStats` 计数器 | 22 |
| `closeConn` / `parkClose` / `teardownClosed` 调用点 | 26（不含声明） |
| 关闭原因字面量（`const char*`） | 14 种 |
| `epoll_ctl` 直接调用 | 10 处 |
| `closing` 引用 / `pendingBytes` 引用 / `isClosed` 引用 | 35 / 25 / 16 |

注释率是这份代码最诚实的度量：**每 2.5 行代码配 1 行注释**，说明作者在持续用散文补偿结构缺失的表达力。这不是坏习惯——它让这个文件比同规模 C++ 可读得多——但它是一个信号：注释在承担本应由类型承担的工作。

---

## 2. 分层视图

| 层 | 行范围 | 内容 | 公开程度 |
|---|---|---|---|
| L0 协议编解码 | 46–97 | `FRAME_MAGIC` / `writeFrameHeader` / `pongFrame` / `encodeFrameInto` | `namespace industrial` 全局可见 |
| L1 基础设施 | 99–234 | `FdGuard`、`RingBuffer` | 全局可见 |
| L2 观测 | 236–295 | `ServerStats`（22 个 atomic） | 全局可见 |
| L3 连接状态 | 297–486 | `SendResult`、`Admission`、`Conn`、`DrainOutcome`、`ClosedConn` | 全局可见 |
| L4 反应堆 | 488–620 + 882–2396 | `SubReactor` 声明 + 实现 | 全局可见（注释写"internal interface"） |
| L5 服务器 | 622–880 + 2398–2988 | `MultiReactorServer` | 公开 API |

### 2.1 L3/L4 边界是名义上的

`Conn` 的 23 个字段按归属可分为四组：

| 组 | 字段 | 数量 | 归属 |
|---|---|---|---|
| 标识 | `id` `fd` `peer` `owner` | 4 | 服务器 / 反应堆 |
| IO 缓冲 | `rbuf` `wbuf` `writeArmed` `sendFrameRemain` | 4 | 反应堆线程独占 |
| **关闭状态机** | `closing` `finSent` `deferredRound` `closeInputBytes` `drainReadBytes` `peerClosed` `closeStartedAtMs` `finSentAtMs` | **8** | 反应堆线程独占（`closing` 例外，跨线程读） |
| **调度 + 配额** | `readPending` `readNext` `readPrev` `pendingBytes` `admission` `isClosed` | **6** | 反应堆 + 跨线程 |

**8 + 6 = 14 个字段（61%）不是连接本身的属性，是 `SubReactor` 状态机的寄存器。** `Conn` 因此既不是数据对象也不是行为对象，它是"被结构体化的调用栈"。

后果可度量：`Conn` 每增加一个字段，`SubReactor` 的 5 个关闭函数都要重新审一遍。R8-1（`unregisterConn` 清 `closing`）之所以能成为一个真实缺陷，正是因为 `closing` 的生命周期跨越了两个类的边界，而没有任何一处代码同时拥有它的全部写入点。

---

## 3. 依赖方向：L4 ↔ L5 循环

名义方向是 L5 → L4。实际存在反向边：

```
MultiReactorServer ──owns──> SubReactor        (正向：reactors_)
SubReactor ──server_──────> MultiReactorServer (反向：裸指针 + friend)
Conn ──owner──────────────> SubReactor         (L3 → L4 反向)
```

`SubReactor` 通过 `friend class SubReactor`（L794）直接访问 L5 的私有成员：

| 被访问的 L5 私有成员 | 访问点 |
|---|---|
| `server_->cfg_` | `submitNewConnection`、`drainWbufToSocket`、`parseFrames`（PONG 配额） |
| `server_->stats_`（经 `statsRef()`） | 45 处 |
| `server_->msgHandler_` / `stateHandler_` / `closeHandler_` | `parseFrames`、`flushCloseCallbacks` |
| `server_->onConnectionOpened/Closed` | 注册 / 注销 |

也就是说：**这两个类在结构上是同一个组件，只是被切成两块写。** 这不是"故意设计的紧耦合"，而是没有被命名的中间概念——一个"反应堆运行上下文"（只读配置 + 计数器 + 回调 + 连接注册表）缺失了。

补上这个概念后，`friend` 可以取消，L4 只依赖一个 6 成员的只读结构，测试也能在没有 `MultiReactorServer` 的情况下构造反应堆。

---

## 4. 关闭协议：最贵的那部分

### 4.1 现状

**状态变量（7 个 + 1 个互斥标志）**

| 变量 | 类型 | 语义 |
|---|---|---|
| `isClosed` | `atomic<bool>` | 已从两个注册表摘除 |
| `closing` | `atomic<bool>` | 已请求优雅关闭，拒绝新提交 |
| `finSent` | `bool` | `shutdown(SHUT_WR)` 已返回 |
| `deferredRound` | `enum{None,Close,PeerClosed}` | 谁欠一个队列轮次 |
| `closeInputBytes` | `int64_t` | 关闭时仍欠对端的输入字节预算 |
| `drainReadBytes` | `size_t` | 本轮读取字节数（计费） |
| `peerClosed` | `bool` | 已收到对端 FIN |

**转移函数（5 个，互相调用）**

```
closeConn ──> parkClose ──> drainWbufToSocket
    │              └──────> tryBeginWriteShutdown
    └──────────> teardownClosed

drainInputThenClose ──> handleRead ──> finishPeerClosed ──> enqueue(自身)
                                                └────────> teardownClosed
handleWrite ──> drainWbufToSocket / finishPeerClosed
```

**入口（26 个）**：`closeConn` 20 处、`parkClose` 8 处、`teardownClosed` 10 处（有重叠，因为前两者内部会调到后者）。

### 4.2 为什么这是架构问题而不是实现问题

三条证据，全部来自仓库自己的记录：

1. `Conn` 里 `deferredRound` 的注释原文：「One bool cannot say which, and conflating them cost the peer its last answer」——**一个字段被拆成两个值，是因为一个 bool 表达不了两种"欠轮次"的语义**。这是状态建模问题。
2. `drainInputThenClose` 的注释原文：「Charging `served * FRAME_HEADER_SIZE` made this a frame counter…」——**预算的单位曾经错了一个量级（帧 vs 字节）**，因为预算本身是个裸 `int64_t`，没有类型保护。
3. 8 轮评审里，case E（0/229432 字节）、case F（11/22 字节）、P0-5（对端半关连接被 RST）、R7-1（close 与 FIN 同读时吞掉刚排队的回复）——**四个独立缺陷，同一个状态机**。

缺陷密度与状态变量数正相关，这在数据上是可见的：关闭协议占全部代码约 18%（约 300 行），却占了 8 轮评审中 5 轮的 P0/P1 条目。

### 4.3 建议的结构

把 7 个字段收进一个子结构，并把转移集中到一个纯函数：

```
struct CloseState {
    Phase phase;              // Open | Closing | FinSent | Torn
    bool  peerFinSeen;
    Round round;              // None | Close | PeerClosed
    Budget inputBudget;       // 强类型，内部是字节
    int64_t startedAtMs, finSentAtMs;
};

// 纯函数，不碰 socket、不碰 epoll、不碰 stats
Transition decide(const CloseState&, CloseEvent);
```

`CloseEvent` 枚举只有 6 个值：`AppClose` / `PeerFin` / `DrainDone` / `BudgetSpent` / `LingerExpired` / `IoError`。
26 个入口退化为 6 个事件投递点。转移表可以在 `verify/` 里直接单测——**不需要 socket、不需要线程、不需要 epoll**，因为它是纯函数。

这是本文最有价值的单条建议：它把一个只能靠 `verify/` 里的 33 种配置、几十秒一轮的集成测试来守的状态机，变成一个毫秒级可穷举的表。

---

## 5. 四重连接索引与全局读锁

同一个"有多少连接"的事实，存在四个来源：

| 来源 | 位置 | 锁 |
|---|---|---|
| `MultiReactorServer::conns_` | L874 | `connsMutex_`（`shared_mutex`） |
| `SubReactor::conns_` | L617 | 无（worker 独占） |
| `SubReactor::fdIndex_` | L616 | 无（worker 独占） |
| `admitted_` | L868 | `atomic<size_t>`（`Admission` RAII） |

四个来源必须一致，靠 `onConnectionOpened` / `onConnectionClosed` 两次回调维持。这是**架构上的重复真相**（duplicated source of truth），代价具体为：

- `sendTo()` 每次调用做：`isRunning()` → `submissionMutex_` 共享锁 → `isRunning()` → `connsMutex_` 共享锁 → `conns_.find()` → `shared_ptr` 拷贝（原子 ++）→ `pendingBytes` CAS → `submitSend` → 任务里 `shared_ptr` 析构（原子 --）。
- **多生产者路径上，每一次发送都要拿两把全局锁、做两次原子引用计数。** 这是这个服务器唯一的跨线程读热点。
- 一致性靠回调维持，意味着"忘记调用 `onConnectionClosed`"是一个可静默的缺陷类别（`discardRejectedConn` 的注释就在描述这一类）。

**建议（需基准验证，不能凭推理改）**：

1. 把 `id → SubReactor*` 作为主索引（无 `shared_ptr`），连接生命周期由 reactor 独占，`shared_ptr` 只在跨线程交接时使用。这样 `sendTo` 不再做两次原子 RMW。
2. 索引按 `id & 15` 分 16 个 shard，每 shard 一把 `shared_mutex`，消掉全局读锁。
3. 验证方式：`verify/bench_send.cpp` 与 `verify/bench_rx.cpp` 前后对比，8 生产者场景。项目规则要求"数字进 commit message"，这条必须遵守。

---

## 6. 单文件契约与内部边界

README 第一段就是这个项目的产品定义：

> copy `include/tcp-server-mt.h` into your project. No `.cpp`, nothing to link.

所以**拆文件不是选项**。但单文件的代价是可度量的，且可以不付：

| 代价 | 是否可缓解 | 缓解方式 |
|---|---|---|
| 每个包含者编译约 3000 行 + 全套系统头 | 部分 | 提供 `INDUSTRIAL_TCP_DECL_ONLY` 编译开关，只含声明段 |
| 无法按层单测 | 是 | `detail` 命名空间隔离 L0–L2，测试直接 include 后访问 |
| 公开符号面 ~20 个，全部落在 `industrial::` | 是 | L0–L3 的多数符号移入 `industrial::detail` |
| 无 TU 边界，无法链接期隔离 | 否 | 接受 |

**公开面收窄清单**（建议保留在 `industrial::` 顶层）：

```
MultiReactorServer, Config, ServerStats, SendResult, FrameType,
FRAME_MAGIC, MAX_FRAME_PAYLOAD, nowMs
```

移入 `detail`：`FdGuard`、`RingBuffer`、`Admission`、`Conn`、`ClosedConn`、`DrainOutcome`、`writeFrameHeader`、`pongFrame`、`encodeFrameInto`。

收益：使用者误用内部类型的可能性归零；`Conn` 的 23 个字段从"公共契约"降级为"实现细节"，可以自由重构。

---

## 7. 其他结构性发现

| ID | 位置 | 问题 | 影响 | 建议 |
|---|---|---|---|---|
| S1 | L2201 | `finishClose(shared_ptr&)` 函数体只有 `c.reset()` | 空抽象层，1 个调用点（L2214），读者要跳过去才发现什么也没做 | 内联删除 |
| S2 | L450–452 | `readPending` / `readNext` / `readPrev` 是侵入式链表节点，直接挂在 `Conn` 上 | 调度器状态污染连接对象；`Conn` 不可重定位 | 抽 `ReadQueue`，节点仍内联但语义归位 |
| S3 | 26 处 | 关闭原因用 `const char*` 字面量，14 种取值 | 不可穷举、不可计数、测试无法断言 | `enum class CloseReason` + `toString()` |
| S4 | L237–295 | 22 个计数器无快照/差分类型 | README 承诺"每次变更记录 before/after"，但没有工具支撑 | `struct StatsSnapshot` + `diff()` |
| S5 | L2469 | `start()` 169 行，承担 8 件事 | 唯一的启动路径，也是最难改的函数 | 拆 `validate()` / `raiseFdLimit()` / `bind()` / `spawnReactors()` / `handshake()` |
| S6 | L2704 | `acceptLoop()` 163 行，承担 7 件事 | fd 建立、accept 循环、准入、socket 调优、peer 格式化、reactor 选择、收尾 | 拆 `setupEpoll()` / `acceptBatch()` / `formatPeer()` / `tuneSocket()` |
| S7 | L744–747 | `failWorkerForTest` / `failAcceptForTest` 是生产类的 public 成员 | 测试钩子进入公共 API | 注入 `FaultHooks` 接口，默认空实现 |
| S8 | L859 | `thread_local callbackServer_` + `CallbackScope` 是隐式全局态 | `SubReactor::stop()`、`~MultiReactorServer()`、`start()` 各自读原始指针判断"是否在回调里" | 收敛为 `Server::isInsideCallback()` 单一 API |
| S9 | 全线 | `noexcept` + `catch(...)` 吞掉异常，失败只进计数器 | 生产环境无法诊断，只有 22 个数字 | 单一 `onInternalFault(code)` 钩子（默认计数，可选日志） |
| S10 | L2499 | `start()` 就地修改 `cfg_.maxPendingBytes` | 配置从"不可变输入"变成"可变状态" | `ResolvedConfig` 独立结构（`resolvedReactorCount()` 已是正确先例） |

---

## 8. 优先级路线图

排序依据：**风险降低 / 改动成本**。每条都给出可在 Linux 上执行的验证判据，遵循项目"无运行时证据不落结论"的规则。

### R1 — 类型化关闭原因（低风险，半天）

`const char*` → `enum class CloseReason`。14 种取值改为枚举，`flushCloseCallbacks` 里转字符串。
验证：现有 9 个 suite 全绿；`grep -c '"' closeConn` 应为 0。

### R2 — 快照与差分（低风险，半天）

`StatsSnapshot` + `diff()`。
验证：`verify/config_budget.cpp` 增加一条断言：任一次 `diff()` 的 `accepted - closed` 等于 `connectionCount()`。

### R3 — 关闭状态机集中化（中风险，2–3 天，收益最大）

`CloseState` 子结构 + `decide()` 纯函数 + `CloseEvent` 枚举。
分两步走：
1. 先只做**提取**（字段搬进子结构，行为不变），跑全部 33 种配置；
2. 再把 5 个函数体改写为"投递事件 + 执行返回的 action"，逐步替换。

验证：新增 `verify/close_state_machine.cpp`，对 `decide()` 做穷举（8 状态 × 6 事件 = 48 格），断言每一格要么是合法转移，要么明确是非法组合。这是这个项目第一次能用毫秒级测试覆盖关闭协议。

### R4 — 拆 `start()` / `acceptLoop()`（中风险，1 天）

纯机械拆分，无行为变化。
验证：`verify/fault_injection.cpp`（启动失败注入）+ `config_budget.cpp` 全绿。

### R5 — 公开面收窄（低风险，半天）

`detail` 命名空间。
验证：`cmake --build` 全绿；`verify/evidence/probe_api_contract.cpp` 需要同步更新——这条 probe 的存在本身就是这个改动的验收标准。

### R6 — 索引重构（中高风险，需基准，2–3 天）

分片 + 去 `shared_ptr`。
验证：`verify/bench_send.cpp` 8 生产者吞吐与 p99 前后对比，数字进 commit message。**没有数字就不合入**。

---

## 9. 明确不建议做的

| 提案 | 否决理由 |
|---|---|
| 拆成 `.h` + `.cpp` 或多头文件 | 破坏 README 的产品承诺（copy one file），这是这个库存在的理由 |
| 引入 asio / libevent 抽象 | README 明确 no third-party dependencies；抽象层的收益抵不过失去的确定性 |
| `Conn` 改成 POD 传给 handler | `sendTo` 依赖 `Conn::owner` 直达路径，改造成本远超收益 |
| `RingBuffer` 改无锁 SPSC | `wbuf` 单线程独占，没有第二个生产者，无收益 |
| 用 `std::expected` 替换 `SendResult` | C++17 目标，且 `SendResult` 的 6 个值已经是充分建模 |
| 全面日志化 | 这个库的定位是"零依赖单文件"，引入日志设施违背定位；S9 的钩子式方案足够 |

---

## 10. 一句话总结

这个文件的代码质量显著高于它的结构质量。注释、不变量说明、单一退出路径的纪律都在平均线以上；欠的是**把已经在注释里写清楚的状态机，翻译成类型**。8 轮评审已经用 4 个真实缺陷为这件事付过账了——R3 是唯一能把这份账结掉的改动。
