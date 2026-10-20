# R3 审计记录

分支：`refactor/r3-close-state-machine`
审计对象：`include/tcp-server-mt.h` 相对 `main` 的 diff（+344 / −104）
判据：`docs/architecture/r3-baseline.md` 第 1、2、5、6 节

审计方与实现方为同一执行体（两个委派 agent 均因 429 限流未产出），因此**这份记录不能算独立审计**。它的价值在于把每一项核对做成可复核的命令，而不是一句"看起来等价"。

---

## E1 字段引用点 — PASS

| 字段 | 新代码 `c->close.*` 行数 | baseline | 差额来源 |
|---|---|---|---|
| `closing` | 10 | 10 | — |
| `finSent` | 8 | 8 | — |
| `round` | 11 | 7 | +4：`parkClose`/`finishPeerClosed`/`drainInputThenClose`/`handleWrite` 各一处只读 facts |
| `inputBudget` | 5 | 4 | +1：`drainInputThenClose` 的 `facts.budgetLeft` 只读 |
| `drainReadBytes` | 4 | 4 | — |
| `peerFinSeen` | 2 | 2 | — |
| `startedAtMs` | 3 | 3 | — |
| `finSentAtMs` | 1 | 1 | — |

5 处增量全部是读，无新增写入点。旧名（`closeInputBytes` / `closeStartedAtMs` / `deferredRound` / `peerClosed` / `DeferredRound`）全文件零残留。

```
grep -c "c->close.round" include/tcp-server-mt.h          # 11
grep -n "closeInputBytes\|closeStartedAtMs\|deferredRound\|peerClosed\|DeferredRound" include/tcp-server-mt.h
```

## E2 原子性 — PASS

`closing`（L389）与 `isClosed`（L571）仍是 `std::atomic<bool>`。`closing` 的 10 处访问全部带 `acquire`/`release`，无 relaxed 混入。

## E3 单调性 — PASS

写入点与 baseline 第 1 节逐行一致：

| 字段 | 写入点 | 次数 |
|---|---|---|
| `peerFinSeen` | L1615 | 1（单调） |
| `finSent` | L1764 | 1（单调） |
| `round` | L1865 / L2316 / L2350 | 3（`None` → `Close`/`PeerClosed`，无回退） |
| `closing` | L2171（true）/ L2051（false） | 2（false 仍只在 `unregisterConn`） |

无新增复位点。

## E4 决策表 26 行 — PASS

三处最危险的重写逐一核对：

**1. `parkClose` 入口 gate**

旧：`if (isClosed || finSent) return;`
新：`if (decide(phase(), AppClose, facts) == None) return;`

`AppClose` 只在 `FinSent` / `Torn` 返回 `None`；`Open` / `Closing` 恒返回 `SendFin` 或 `Park`。等价。

**2. `parkClose` drain 之后**

旧：`if (!wbuf.empty()) armWrite(); else tryBeginWriteShutdown();`
新：按 `decide(AppClose, wbufEmpty)` 分派 `SendFin` → `tryBeginWriteShutdown`，`Park` → `armWrite`。

`AppClose` 在非 `FinSent`/`Torn` 时只可能返回这两个值之一，且与 `wbufEmpty` 一一对应。`switch` 的 `default` 分支在语义上不可达，保留是为了不引入未处理枚举警告。等价。

**3. `handleWrite` 收尾**

旧：`if (closing && !tryBeginWriteShutdown(c)) teardown;`
新：`if (decide(DrainDone) == SendFin) { if (!tryBeginWriteShutdown(c)) teardown; }`

`decide(DrainDone)` 在 `Closing` 且 `wbufEmpty` 时返回 `SendFin`，而此处 `wbuf` 必然为空（上面刚 `return` 掉非空分支），所以外层条件退化为 `phase == Closing`，与原 `closing` 测试一致。短路顺序：先判阶段，再判 `shutdown` 返回值，与旧代码相同。等价。

**`finishPeerClosed` 的四种落点**逐格对照（`wbuf` 空、`round != None` 这一格是旧代码最容易读错的地方）：

| 阶段 | wbuf | round | pending | 旧行为 | 新表 |
|---|---|---|---|---|---|
| Open | 空 | None | 有 | defer 一轮 | DeferPark |
| Open | 空 | None | 无 | graceful | TeardownGraceful |
| Open | 空 | **非 None** | 任意 | graceful | TeardownGraceful |
| Closing | 空 | 任意 | 任意 | tryBeginWriteShutdown | SendFin |
| FinSent | 空 | 任意 | 任意 | graceful | TeardownGraceful |
| 任意 | 非空 | 任意 | 任意 | park | Park |

一致。

## E5 `doTick` linger — PASS

仍读 `c->close.startedAtMs`（L2421–2422），与 `parkClose` 的 L2173 写入配对。`finSentAtMs` 仍无读者（baseline 已标注为死字段，本次未动）。

## E6 回调延迟 — PASS

`teardownClosed` 仍只把 `ClosedConn` 推进 `pendingCloses_`，无新增同步回调调用。

## E7 无新增分配 / 锁 — PASS

diff 中新增行匹配 `make_shared|new |mutex|lock_guard|unique_lock|shared_lock` 的数量为 **0**。

## E8 Linux 编译与集成 — PASS（一项环境性失败，已对照 main 排除）

环境：WSL2 Ubuntu 22.04，`g++ 11.4.0`，8 核，`ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh`。

| 项 | 结果 |
|---|---|
| `-Wall -Wextra -Werror` 编译 `verify/close_state_machine.cpp` | 通过，零警告 |
| cpplint（`include/tcp-server-mt.h`） | **0 findings** |
| plain 配置 | 11/12 绿 |
| asan+ubsan 配置 | 11/12 绿 |
| tsan 配置 | 11/12 绿 |
| 合计 | **33/36** |

**唯一失败项：`harness_selftest`，三个配置各一次。**

失败内容是 D 用例 `expectClose(Rst)`：

```
[FAIL] RST was reported: expected RST, saw FIN
[FAIL] and it is not reported as a FIN
```

这不是本次改动引起的。同一个日志里其它套件对同一件事给的是 `[SKIP]`：

```
[SKIP] no wire RST is observable on this kernel: it ignores SO_LINGER{on,0}
       (checked against a bare socket pair), so case C can only assert the
       server's own evidence here
```

WSL2 内核不把 `SO_LINGER{on,0}` 变成可观测的 RST，于是 harness 自己的 RST 检测器无法被验证。`harness_selftest` 没有做这个前置探测，直接断言，所以它红了。

对照实验（决定性证据）：在 `main`（`6d1eedd`，重构前）上单独编译并运行同一个用例：

```
$ g++ -std=c++17 -Wall -Wextra -Iinclude verify/harness_selftest.cpp -o /tmp/hs_main
$ /tmp/hs_main
D. expectClose(Rst) recognises a reset
  [PASS] peer connected
  [FAIL] RST was reported: expected RST, saw FIN
  [FAIL] and it is not reported as a FIN
```

**逐字相同。** 与本次重构无关，是环境限制。

### 顺带发现的两件事（不属于 R3，未处理）

1. `harness_selftest` 缺少其它套件都有的内核能力探测，所以它在本内核上是硬失败而不是 `[SKIP]`。这是"检查本身无法通过"的检查，按项目自己的标准应当先问机器再断言。
2. `run.sh` 的退出码在本次执行中不可靠（`Result: FAILURES` 但 shell 拿到 0）。所以本次结论是逐行读每个套件的 `rc=` 得出的，不是看脚本退出码。

### 行尾问题（环境）

工作树是 CRLF（`core.autocrlf=true`，仓库无 `.gitattributes`），`set -euo pipefail\r` 在 bash 下直接报 `invalid option name`。验证在 `/tmp` 的 LF 副本上完成，内容与分支一致。**这只影响 Windows 侧检出后直接在 WSL 里跑**，不影响 CI。

---

## 审计过程中发现并修掉的三个问题

这三条是这次重构自己引入的，不是原有缺陷。

### A1 `decide(DrainDone)` 不检查 `wbufEmpty`（阻塞，已修）

第一版只判阶段：`Closing → SendFin`。测试里的手写断言 `no FIN over a non-empty wbuf (DrainDone)` 失败。

穷举测试没抓到它，因为**预言机是同一份表的第二次抄写**，两处都漏了同一个守卫。抓到它的是那条独立写出来的安全性质断言。这是"表对表"测试的盲区，值得记下来。

修法：`decide` 里把 `!f.wbufEmpty` 并入条件，返回 `None` 而不是 `SendFin`。预言机同步加守卫，并在注释里注明"此处比 baseline 第 5 节更严，是契约的一部分"。

### A2 / A3 defer 分支失败后 `facts.round` 未同步（非阻塞，已修）

`finishPeerClosed` 与 `drainInputThenClose` 里，defer 分支先置 `round` 再 `enqueue`；`enqueue` 失败时函数继续往下走，而 `facts.round` 还是进入时的值，后面的 `expectClose` 会误报一次 `closeStateMismatch`。

这条在集成测试里表现为"队列打满时计数器莫名非零"，极难追。修法：置位后同步 `facts.round = c->close.round;`。

---

## 结论

E1–E8 通过。33/36 绿，唯一红项在 `main` 上逐字复现，是内核能力问题而非代码问题。

代码层面等价性成立，运行时等价性有集成证据支撑：plain / asan+ubsan / tsan 三种配置下，`close_semantics`（含 `CS_REACTORS=1`）、`idle_semantics`、`ping_pong`、`send_results`、`backpressure`、`fault_injection`、`reactor_spread`（两种 reactor 数）、`config_budget` 全部通过——也就是八轮评审落过的每一个关闭协议现场，都在重构后仍然成立。

**`closeStateMismatch` 的证据**：`close_semantics.cpp` 的 D 段新增一条断言，运行结束后 `closeStateMismatch == 0`。它必须在这里而不是在 `close_state_machine.cpp`，因为只有真实运行才会走到 `expectClose()`。

断言的有效性做了负向验证：把 `finishPeerClosed` 里一处 `expectClose(..., Park, ...)` 故意改成 `SendFin`，同一个套件立刻红：

```
[FAIL] decide() and the close branches agreed on every transition
FAILURES (1 failure)
```

还原后 `ALL PASS (0 failures)`。所以这条断言不是"永远为真"的装饰。
