# R4–R6 执行记录

分支：`refactor/r4-r6-structure`（本地，未推送）
起点：`refactor/r3-close-state-machine` 的 HEAD
环境：WSL2 Ubuntu 22.04 / g++ 11.4.0 / 8 核

| 项 | 结果 |
|---|---|
| R4 拆 `start()` / `acceptLoop()` | 已合入 |
| R5 收窄公开面 | 已合入 |
| R6 分片连接索引 | **实测无收益，已回滚** |
| 前置修复 `bench.sh` | 已合入 |

---

## 前置：`verify/bench.sh` 从未产出过完整结果

建立 R6 基线时发现汇总脚本崩溃：

```
statistics.StatisticsError: no median for empty data
```

原因：`bench_rx` 输出 `mode=same`，汇总循环只跳过了 `connections/reactors/clients/payload` 四个字段。`mode` 进入 `float('same')` 抛异常，而 `groups[key][name]` 在异常前已经被 `defaultdict` 创建成空列表，`median()` 随即死在上面。

后果：任何带 `mode=` 的输出都会让汇总中断在 `ingest reactors=1` 之后。**这个项目声称用来产出数字的脚本，从来没有产出过完整数字。**

修法是一行（把 `mode` 加进跳过列表）。形状与另外两处相同——cpplint 的对照文件、`harness_selftest` 的 RST 用例——**一个不可能通过的检查，读起来和一个通过的检查一模一样。**

---

## R4 — 拆 `start()` 与 `acceptLoop()`

`start()` 180 行 8 件事 → `validateConfig` / `warnCoarseTick` / `clampConfig` / `ensureFdLimit` / `spawnReactors` / `startAcceptThread` / `rollbackStart`。
`acceptLoop()` 163 行 7 件事 → `setupAcceptEpoll` / `acceptBatch` / `formatPeer` / `tuneAcceptedSocket`。

两处专门核对过的地方：

1. `validateConfig()` 的 `||` 链保持原顺序，包括两个读 `cfg_` 的容量比较。
2. `startAcceptThread()` 只在 promise 交付了值时返回 `!failed_`，所以握手超时仍然落到 `rollbackStart()`，而不是误报成功。

验证：33/36，与基线逐项相同（红的 3 个是 `harness_selftest`，WSL2 内核忽略 `SO_LINGER{on,0}`，已在 `main` 上复现）。cpplint 0。

## R5 — 收窄公开面

`Conn`、`SubReactor`、`FdGuard`、`RingBuffer`、`Admission`、`ClosedConn`、`DrainOutcome`、`CloseState` 及其枚举、`decide()`、`pongFrame()` 移入 `industrial::detail`。

**两处刻意留在公开面**：`encodeFrameInto` 与 `writeFrameHeader`。理由是实测出来的——先都移进 detail，`idle_semantics.cpp` 编译失败，它需要 `writeFrameHeader` 来构造半帧。二者都是协议编解码入口，是"使用者手里拿着字节"的那一面。缓存的 PONG 不是，所以 `pongFrame` 移走了。

`onConnectionOpened/Closed` 转 private：它们的签名里有 `detail::Conn`，留 public 等于把刚移走的类型又放回表面。

验证手段换了：`probe_api_contract.cpp` 只碰公开面，R5 之后它**照常编译**，所以它不是这次改动的哨兵。新增 `verify/evidence/probe_surface_shrank.sh`：13 个必须编译失败的 TU + 1 个必须编译成功的对照。在命名空间作用域注入 `using detail::Conn;` 会让它变红——这是它确实在测量的证据。

验证：33/36，cpplint 0，公开面探针 PASS。

---

## R6 — 分片连接索引：实测无收益，回滚

### 做了什么

`sendTo()` 每次调用要拿两把全局共享锁（`submissionMutex_` + `connsMutex_`）。把连接表按 `id & 15` 分 16 个 shard，每 shard 一把 `shared_mutex`，去掉全局读锁。

**`shared_ptr` 保留。** 原计划是改成裸 `Conn*` 以省掉两次原子引用计数，但代码读下来没有依据：`teardownClosed()` 在所属 reactor 上抹掉连接时，`sendTo()` 可能已经握住了指针，而这个文件里没有 epoch 或 hazard pointer 能跨过那个窗口。**为基准数字换进一个需要特定交错才触发的 use-after-free，不是交易。**

### 实测

同机、同脚本、`REPS=3`、前后相隔约 1 分钟。对照是 R5 树，不是 20 分钟前的旧基线（那一份的机器状态已经漂移，见下）。

| 指标 | R5 | R6 | 区间是否重叠 |
|---|---|---|---|
| rss per_conn_kb | 128.70 | 128.70 | 同值 |
| ingest 1 reactor | 288.10 MB/s | 335.20 MB/s | 重叠（R5 286–442） |
| ingest 2 reactors | 1119.60 MB/s | 1020.40 MB/s | 重叠（R6 903–1039） |
| ingest 4 reactors | 942.90 MB/s | 1018.10 MB/s | 重叠（R5 877–957） |
| send p50 | 76.60 µs | 76.40 µs | 重叠 |
| send p99 | 150.30 µs | 143.30 µs | 重叠（R6 136–174） |
| send mean | 79.70 µs | 80.90 µs | 重叠 |

全部落在噪声内。三配置套件 33/36 绿、cpplint 0，**正确性没问题，但没有任何一项能证明它更快。**

### 为什么测不出来（这条比结论重要）

`bench_send.cpp` 是**单连接**往返：1 个 peer、2000 条消息。`connsMutex_` 在这种负载下根本没有竞争者，分片自然无从体现。

也就是说：**这次基准测的不是 R6 假设的那条路径。** 没有观测到差异，不等于没有差异；它等于这份基准问不出这个问题。

要继续 R6，前置条件是一个真正的多生产者基准：N 个线程 × M 个连接并发 `sendTo`，测 p99 与吞吐。在那之前，R6 不值得合入——按项目自己的规则，"没有数字不合入"。

顺带一个观察：`sendTo()` 每条消息都要 `std::vector<uint8_t> frame` 分配一次。在单连接基准里这个开销很可能盖过了锁，也是分片效果被淹没的另一个原因。这一条没有验证，只是读代码读到的。

---

## 三个 agent

定义在 `.workbuddy-ai/agents/`：`r46-coding-agent` / `r46-test-agent` / `r46-audit-agent`。

**三个都没能跑起来。** coding agent 在 13 秒后 429 退出，与前一轮的 impl/audit 同样原因。实现、测试、审计全部由主执行体完成。

这不是"agent 不好用"的结论，是这个会话的配额结论。agent 定义保留，因为其中的 brief（每一步的验收判据、已知的环境红、验证命令）本身是可复用的资产。
