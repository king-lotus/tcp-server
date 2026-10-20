# R3 基线测绘：关闭协议现状

分支：`refactor/r3-close-state-machine`
基线：`include/tcp-server-mt.h` @ `6d1eedd`（2989 行）
用途：R3 重构的**等价性判据**。重构前后，本文每一格都必须保持成立。

---

## 1. 字段全量引用点

写入点（W）与读取点（R）逐行列出。重构后每个 W/R 必须仍存在，只是换了载体。

### 1.1 `closing`（`std::atomic<bool>`，L379）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1954 | `parkClose`：置 true（仅当原本 false） |
| W | 1851 | `unregisterConn`：置 false（**唯一复位点**） |
| R | 1157 | `submitSend` 任务：拒绝新帧 |
| R | 1561 | `handleWrite`：wbuf 空后决定是否 FIN |
| R | 1668 | `finishPeerClosed`：判断是否要 defer 一轮 |
| R | 1686 | `finishPeerClosed`：是否发 FIN |
| R | 1765 | `parseFrames` PONG：关闭中丢弃 PONG |
| R | 1953 | `parkClose`：是否首次进入 |
| R | 2267 | `doTick`：关闭中的连接走 linger 分支 |
| R | 2910 | `sendTo`：关闭中返回 `Closed` |

跨线程读取：1157（worker 任务内）、2910（任意应用线程）。**必须保持 atomic。**

### 1.2 `finSent`（`bool`，L380）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1579 | `tryBeginWriteShutdown`：`shutdown()` 成功后置 true |
| R | 1486 | `handleWrite`：FIN 后的陈旧 EPOLLOUT |
| R | 1573 | `tryBeginWriteShutdown`：幂等返回 |
| R | 1686 / 1926 / 1952 / 2015 / 2028 | 五处前置判断 |

**单调**：只从 false 变 true，全文件无复位。

### 1.3 `deferredRound`（`DeferredRound`，L398）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1671 | `finishPeerClosed`：`PeerClosed` |
| W | 2083 | `drainInputThenClose`：`Close`（pendingBytes > 0 分支） |
| W | 2109 | `drainInputThenClose`：`Close`（默认分支） |
| R | 1669 | `finishPeerClosed`：仅 `None` 可进入 |
| R | 2081 | `drainInputThenClose`：仅 `None` 可进入 |
| R | 2102 | `drainInputThenClose`：`PeerClosed` 直接返回 |
| R | 2104 | `drainInputThenClose`：`Close` 已用则直接 park |

**单调**：`None` → `Close` / `PeerClosed`，无回退。

### 1.4 `closeInputBytes`（`int64_t`，L407）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 2019 | 首次测量：`rbuf.readable()`（哨兵 `-1` 表示未测） |
| W | 2054 | 按实际服务的字节扣减 |
| R | 2018 | 是否已测 |
| R | 2062 | 预算是否还允许下一轮 |

**不是状态，是预算**。语义为"关闭时仍欠对端的输入字节数"。

### 1.5 `drainReadBytes`（`size_t`，L412）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1428 | `handleRead`：每次 `recv` 累加 |
| W | 2021 / 2055 | `drainInputThenClose`：每轮清零 |
| R | 2052 | 计费公式 |

**不是状态，是本轮计费累加器。**

### 1.6 `peerClosed`（`bool`，L416）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1447 | `handleRead`：`recv == 0` |
| R | 1510 | `handleWrite`：drain 完成后据此收尾 |

**单调**。

### 1.7 `isClosed`（`std::atomic<bool>`，L421）

| 类型 | 行 | 上下文 |
|---|---|---|
| W | 1871 | `discardRejectedConn`：置 true（未注册就拒绝） |
| W | 2136 | `teardownClosed`：CAS false→true（唯一正常出口） |
| R | 1156 / 1398 / 1618 / 1787 / 1952 / 2015 / 2028 / 2360 / 2909 | 九处 |

跨线程读取：1156、2909。**必须保持 atomic。** 单调，无复位。

### 1.8 时间戳

| 字段 | W | R |
|---|---|---|
| `closeStartedAtMs` | 1955（`parkClose` 首次进入） | 2274–2275（`doTick` linger 判定） |
| `finSentAtMs` | 1580 | **无读者** |

`finSentAtMs` 目前是死字段：只写不读。R3 保留它（诊断价值），但需在注释里承认这一点，否则下一个人会以为漏了逻辑。

---

## 2. 不变式（重构必须保持）

| # | 不变式 | 依据 | 现状是否被强制 |
|---|---|---|---|
| I1 | `finSent ⇒ closing` | `tryBeginWriteShutdown` 的两个调用点（L1562、L1691）都以 `closing` 为前置 | 是，但靠调用点，非类型 |
| I2 | `isClosed ⇒ fd == -1` | `teardownClosed` 先 CAS `isClosed`，再 `unregisterConn` 置 `fd = -1` | 是 |
| I3 | `isClosed` 单调，且只有一个 CAS 出口（L2136）+ 一个拒绝路径（L1871） | 代码结构 | 是 |
| I4 | `deferredRound` 至多从 `None` 走一次 | L1669、L2081 都以 `None` 为前置 | 是 |
| I5 | `closeInputBytes` 只减不增（初始化那一次除外） | L2054 唯一扣减点 | 是 |
| I6 | `closing` 为 false 时 `closeStartedAtMs == 0` 或为上一轮残留（连接不复用，故无影响） | 连接对象一次性 | 是（隐式） |

**I1 目前没有被任何断言保护**，且它是"FIN 早于内容发出"这类缺陷的根。R3 应把它变成一个可执行的检查。

---

## 3. 阶段派生

四个阶段由三个单调布尔派生，无需新增存储：

| 阶段 | 条件 | 语义 |
|---|---|---|
| `Open` | `!closing && !finSent && !isClosed` | 正常读写，接受提交 |
| `Closing` | `closing && !finSent && !isClosed` | 拒绝新提交，欠对端的字节仍在发 |
| `FinSent` | `finSent && !isClosed` | FIN 已出，等对端 FIN 或 linger |
| `Torn` | `isClosed` | 已从两个注册表摘除，fd 已关 |

`finSent` 阶段不检查 `closing`，因为 I1 保证它恒真。

---

## 4. 事件到现有分支的映射

| 事件 | 现有触发点 | 行 |
|---|---|---|
| `AppClose` | `closeConnection` → `submitClose` → `drainInputThenClose` | 2944–2969 → 1228 |
| `PeerFin` | `recv` 返回 0 | 1447 |
| `DrainDone` | `drainWbufToSocket` 返回 `Drained` | 1506、1971 |
| `BudgetSpent` | `handleRead` 帧/字节预算耗尽；`closeInputBytes` 归零 | 1399、2062 |
| `LingerExpired` | `doTick` 的 linger 分支 | 2274 |
| `IoError` | `DrainOutcome::Dead/Error`、`epoll_ctl` 失败、`shutdown` 失败 | 1490、1501、1566、1658、1960、1968 |

---

## 5. 现状决策表（逐函数）

这是重构的对照物。**每一行的"结果"都必须在新代码中可复现。**

### `parkClose`（L1951）

| 前置 | 结果 |
|---|---|
| `Torn` 或 `FinSent` | 无操作 |
| `Open` | 置 `Closing`，记 `closeStartedAtMs`，继续 |
| drain 返回 `Error` | `Teardown(abort)` |
| drain 返回 `Dead` | `Teardown(abort)` |
| `wbuf` 非空 | `ArmWrite`（park） |
| `wbuf` 空 | `TrySendFin` |

### `tryBeginWriteShutdown`（L1572）

| 前置 | 结果 |
|---|---|
| `finSent` 或 `fd < 0` | 返回 `finSent` |
| `wbuf` 非空 | 返回 false（不发 FIN） |
| `shutdown` 失败 | 返回 false |
| 成功 | `finSent = true`，记 `finSentAtMs` |

### `handleWrite`（L1483）

| 前置 | 结果 |
|---|---|
| `finSent` | `DisarmWrite` |
| drain `Error` | `Teardown(abort)` |
| drain `Dead` | `Teardown(abort)` |
| `wbuf` 非空（`Stalled`） | 保持 arm，等 EPOLLOUT |
| `peerClosed` | `finishPeerClosed` |
| `Closing` | `DisarmWrite` + `TrySendFin`（失败则 abort） |
| 其余 | `DisarmWrite` |

### `finishPeerClosed`（L1617）

| 前置 | 结果 |
|---|---|
| `Torn` | 无操作 |
| `wbuf` 非空，EPOLLOUT 注册成功 | `Park`（丢 EPOLLIN） |
| `wbuf` 非空，注册失败 | `Teardown(abort)` |
| `wbuf` 空、`!Closing`、`round == None`、`pendingBytes > 0` | `DeferRound(PeerClosed)` + 重入 |
| `Closing && !finSent` | `TrySendFin`，失败则 abort |
| 其余 | `Teardown(graceful)` |

### `drainInputThenClose`（L2014）

| 前置 | 结果 |
|---|---|
| `Torn` 或 `FinSent` | 无操作 |
| `handleRead` 后已关闭 | 无操作 |
| `served > 0 && rbuf 非空 && budget > 0` | 再入一轮 |
| `served == 0 && round == None && pendingBytes > 0` | `DeferRound(Close)` + `parkClose` |
| `round == PeerClosed` | 交回该轮次持有者 |
| `served == 0 \|\| round == Close` | `parkClose` |
| 其余 | `DeferRound(Close)` + `parkClose` |

---

## 6. 等价性判据（重构后逐条核验）

| # | 判据 |
|---|---|
| E1 | 9 个字段的每一个 W/R 行，在新代码中仍有对应位置（`git diff` 逐 hunk 核） |
| E2 | `closing` / `isClosed` 仍是 `std::atomic`，仍由 `acquire`/`release` 访问 |
| E3 | `finSent` / `deferredRound` / `peerClosed` 的单调性不变 |
| E4 | 第 5 节决策表的 26 行，每行结果一致 |
| E5 | `doTick` 的 linger 判定仍读同一个时间戳 |
| E6 | 关闭回调仍全部延迟到 `flushCloseCallbacks` |
| E7 | 无新增动态分配、无新增锁 |
| E8 | `-Wall -Wextra -Werror` 零警告（Linux 侧） |

**本地不可验证项**：E8 的 Linux 编译、以及全部集成套件（33 配置）。本机无 Linux 环境（WSL 被安全策略阻断），只有 `msys64/ucrt64 g++ 15.2.0`。可本地验证的只有纯函数部分，见 `verify/evidence/extract_close_sm.py`。
