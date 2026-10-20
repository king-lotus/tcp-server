# R3 baseline: the close protocol as it stands

Branch: `refactor/r3-close-state-machine`
Baseline: `include/tcp-server-mt.h` @ `6d1eedd` (2989 lines)
Purpose: the **equivalence criteria** for the R3 refactor. After the refactor, every cell in
this document must still hold.

---

## 1. Every reference to every field

Write sites (W) and read sites (R), listed line by line. After the refactor each W/R must
still exist; only the carrier changes.

### 1.1 `closing` (`std::atomic<bool>`, L379)

| Kind | Line | Context |
|---|---|---|
| W | 1954 | `parkClose`: set true (only if it was false) |
| W | 1851 | `unregisterConn`: set false (**the only reset**) |
| R | 1157 | `submitSend` task: refuse new frames |
| R | 1561 | `handleWrite`: decide whether to FIN once wbuf is empty |
| R | 1668 | `finishPeerClosed`: decide whether to defer a round |
| R | 1686 | `finishPeerClosed`: whether to send the FIN |
| R | 1765 | `parseFrames` PONG: drop the PONG while closing |
| R | 1953 | `parkClose`: is this the first entry |
| R | 2267 | `doTick`: a closing connection takes the linger branch |
| R | 2910 | `sendTo`: return `Closed` while closing |

Cross-thread reads: 1157 (inside a worker task) and 2910 (any application thread).
**Must stay atomic.**

### 1.2 `finSent` (`bool`, L380)

| Kind | Line | Context |
|---|---|---|
| W | 1579 | `tryBeginWriteShutdown`: set true after `shutdown()` succeeds |
| R | 1486 | `handleWrite`: a stale EPOLLOUT after the FIN |
| R | 1573 | `tryBeginWriteShutdown`: idempotent return |
| R | 1686 / 1926 / 1952 / 2015 / 2028 | five precondition checks |

**Monotonic**: false → true only; no reset anywhere in the file.

### 1.3 `deferredRound` (`DeferredRound`, L398)

| Kind | Line | Context |
|---|---|---|
| W | 1671 | `finishPeerClosed`: `PeerClosed` |
| W | 2083 | `drainInputThenClose`: `Close` (pendingBytes > 0 branch) |
| W | 2109 | `drainInputThenClose`: `Close` (default branch) |
| R | 1669 | `finishPeerClosed`: only `None` may enter |
| R | 2081 | `drainInputThenClose`: only `None` may enter |
| R | 2102 | `drainInputThenClose`: `PeerClosed` returns immediately |
| R | 2104 | `drainInputThenClose`: `Close` already spent → park immediately |

**Monotonic**: `None` → `Close` / `PeerClosed`, never back.

### 1.4 `closeInputBytes` (`int64_t`, L407)

| Kind | Line | Context |
|---|---|---|
| W | 2019 | first measurement: `rbuf.readable()` (sentinel `-1` means unmeasured) |
| W | 2054 | decrement by the bytes actually served |
| R | 2018 | has it been measured yet |
| R | 2062 | does the budget still allow another round |

**Not state, a budget.** Its meaning is "input still owed to the peer at close time".

### 1.5 `drainReadBytes` (`size_t`, L412)

| Kind | Line | Context |
|---|---|---|
| W | 1428 | `handleRead`: added on every `recv` |
| W | 2021 / 2055 | `drainInputThenClose`: zeroed each round |
| R | 2052 | the charging formula |

**Not state; a per-round accumulator for billing.**

### 1.6 `peerClosed` (`bool`, L416)

| Kind | Line | Context |
|---|---|---|
| W | 1447 | `handleRead`: `recv == 0` |
| R | 1510 | `handleWrite`: finishes accordingly once the drain completes |

**Monotonic.**

### 1.7 `isClosed` (`std::atomic<bool>`, L421)

| Kind | Line | Context |
|---|---|---|
| W | 1871 | `discardRejectedConn`: set true (rejected before registration) |
| W | 2136 | `teardownClosed`: CAS false→true (the only normal exit) |
| R | 1156 / 1398 / 1618 / 1787 / 1952 / 2015 / 2028 / 2360 / 2909 | nine sites |

Cross-thread reads: 1156, 2909. **Must stay atomic.** Monotonic, no reset.

### 1.8 Timestamps

| Field | W | R |
|---|---|---|
| `closeStartedAtMs` | 1955 (first entry to `parkClose`) | 2274–2275 (the linger test in `doTick`) |
| `finSentAtMs` | 1580 | **no reader** |

`finSentAtMs` is currently a dead field: written, never read. R3 keeps it (diagnostic value)
but has to admit that in a comment, or the next reader will assume some logic is missing.

---

## 2. Invariants (the refactor must preserve these)

| # | Invariant | Basis | Currently enforced? |
|---|---|---|---|
| I1 | `finSent ⇒ closing` | both call sites of `tryBeginWriteShutdown` (L1562, L1691) take `closing` as a precondition | yes, but by convention at the call sites, not by the type |
| I2 | `isClosed ⇒ fd == -1` | `teardownClosed` CASes `isClosed` first, then `unregisterConn` sets `fd = -1` | yes |
| I3 | `isClosed` is monotonic, with one CAS exit (L2136) plus one rejection path (L1871) | code structure | yes |
| I4 | `deferredRound` moves out of `None` at most once | L1669 and L2081 both require `None` | yes |
| I5 | `closeInputBytes` only decreases (apart from the initialisation) | L2054 is the only decrement | yes |
| I6 | when `closing` is false, `closeStartedAtMs` is 0 or left over from a previous life (connections are not reused, so this has no effect) | the connection object is single-use | yes (implicitly) |

**I1 is currently protected by no assertion at all**, and it is the root of the "FIN before
content" class of defect. R3 should turn it into an executable check.

---

## 3. Phase derivation

Four phases derived from three monotonic booleans, with no new storage:

| Phase | Condition | Meaning |
|---|---|---|
| `Open` | `!closing && !finSent && !isClosed` | normal read/write, submissions accepted |
| `Closing` | `closing && !finSent && !isClosed` | new submissions refused, bytes owed are still going out |
| `FinSent` | `finSent && !isClosed` | FIN is out, waiting for the peer's FIN or for linger |
| `Torn` | `isClosed` | removed from both registries, fd closed |

The `finSent` phase does not test `closing`, because I1 guarantees it.

---

## 4. Mapping events onto the existing branches

| Event | Existing trigger | Line |
|---|---|---|
| `AppClose` | `closeConnection` → `submitClose` → `drainInputThenClose` | 2944–2969 → 1228 |
| `PeerFin` | `recv` returns 0 | 1447 |
| `DrainDone` | `drainWbufToSocket` returns `Drained` | 1506, 1971 |
| `BudgetSpent` | `handleRead` exhausts its frame/byte budget; `closeInputBytes` hits zero | 1399, 2062 |
| `LingerExpired` | the linger branch in `doTick` | 2274 |
| `IoError` | `DrainOutcome::Dead/Error`, a failed `epoll_ctl`, a failed `shutdown` | 1490, 1501, 1566, 1658, 1960, 1968 |

---

## 5. The decision tables as they stand (function by function)

This is what the refactor is checked against. **Every row's outcome must be reproducible in
the new code.**

### `parkClose` (L1951)

| Precondition | Outcome |
|---|---|
| `Torn` or `FinSent` | no-op |
| `Open` | set `Closing`, record `closeStartedAtMs`, continue |
| drain returns `Error` | `Teardown(abort)` |
| drain returns `Dead` | `Teardown(abort)` |
| `wbuf` non-empty | `ArmWrite` (park) |
| `wbuf` empty | `TrySendFin` |

### `tryBeginWriteShutdown` (L1572)

| Precondition | Outcome |
|---|---|
| `finSent` or `fd < 0` | return `finSent` |
| `wbuf` non-empty | return false (no FIN) |
| `shutdown` fails | return false |
| success | `finSent = true`, record `finSentAtMs` |

### `handleWrite` (L1483)

| Precondition | Outcome |
|---|---|
| `finSent` | `DisarmWrite` |
| drain `Error` | `Teardown(abort)` |
| drain `Dead` | `Teardown(abort)` |
| `wbuf` non-empty (`Stalled`) | stay armed, wait for EPOLLOUT |
| `peerClosed` | `finishPeerClosed` |
| `Closing` | `DisarmWrite` + `TrySendFin` (abort on failure) |
| otherwise | `DisarmWrite` |

### `finishPeerClosed` (L1617)

| Precondition | Outcome |
|---|---|
| `Torn` | no-op |
| `wbuf` non-empty, EPOLLOUT registration succeeds | `Park` (drop EPOLLIN) |
| `wbuf` non-empty, registration fails | `Teardown(abort)` |
| `wbuf` empty, `!Closing`, `round == None`, `pendingBytes > 0` | `DeferRound(PeerClosed)` + re-enter |
| `Closing && !finSent` | `TrySendFin`, abort on failure |
| otherwise | `Teardown(graceful)` |

### `drainInputThenClose` (L2014)

| Precondition | Outcome |
|---|---|
| `Torn` or `FinSent` | no-op |
| already closed after `handleRead` | no-op |
| `served > 0 && rbuf non-empty && budget > 0` | another round |
| `served == 0 && round == None && pendingBytes > 0` | `DeferRound(Close)` + `parkClose` |
| `round == PeerClosed` | hand back to the owner of that round |
| `served == 0 \|\| round == Close` | `parkClose` |
| otherwise | `DeferRound(Close)` + `parkClose` |

---

## 6. Equivalence criteria (checked one by one after the refactor)

| # | Criterion |
|---|---|
| E1 | every W/R line of the nine fields still has a corresponding place in the new code (checked hunk by hunk in `git diff`) |
| E2 | `closing` / `isClosed` are still `std::atomic`, still accessed with `acquire`/`release` |
| E3 | the monotonicity of `finSent` / `deferredRound` / `peerClosed` is unchanged |
| E4 | every row of the section 5 decision tables produces the same outcome |
| E5 | the linger test in `doTick` still reads the same timestamp |
| E6 | close callbacks are still all deferred to `flushCloseCallbacks` |
| E7 | no new dynamic allocation, no new locks |
| E8 | `-Wall -Wextra -Werror` with zero warnings (Linux) |

**Items not verifiable locally at the time of writing**: the Linux build for E8, and the whole
integration suite (33 configurations). There was no Linux environment on this machine (WSL
was blocked by security policy) and only `msys64/ucrt64 g++ 15.2.0`; the pure-function part
was all that could be checked locally, via `verify/evidence/extract_close_sm.py`.

> WSL was unblocked later in the same session, and both E8 and the integration runs were
> completed — see `docs/architecture/r3-audit.md` §E8 for the results.
