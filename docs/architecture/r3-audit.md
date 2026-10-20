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

## E8 Linux 编译 — 未验证

本机无 Linux（`wsl.exe` 被安全中心程序黑名单阻断）。已做的是：

- `msys64/ucrt64 g++ 15.2.0`，`-std=c++17 -Wall -Wextra` 编译**纯函数段**，零警告；
- 全文件括号平衡检查（去注释与字符串后 `{}` `()` `[]` 均归零，无负深度）。

**未做**：整个头文件在 Linux 上的编译，以及 `verify/run.sh` 的 33 种配置。

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

E1–E7 通过，E8 未验证。

代码层面等价性成立；**运行时等价性未经集成验证**。项目规则是"无运行时证据不落结论"，所以这次改动在 `wsl.exe` 放行、`verify/run.sh` 跑绿之前，只能算"结构已就位、行为待证"。
