# Architecture analysis: `include/tcp-server-mt.h`

Subject: `include/tcp-server-mt.h` (2989 lines, 1702 code lines)
Method: static structure analysis + measurement + cross-check against the eight existing
review documents
Nature: an **architectural** assessment. It does not repeat the defect list in `docs/review/`.
It answers "where will this structure keep producing defects", not "line N has a bug".

---

## 0. Conclusions

| Dimension | Finding |
|---|---|
| Layering | Five layers exist, but the L3/L4 boundary is nominal; `Conn` and `SubReactor` share one set of state-machine registers |
| Dependency direction | L4 ↔ L5 is a **two-way cycle**, opened by `friend class SubReactor` |
| Single responsibility | Three clear breaches: `start()` (8 jobs), `acceptLoop()` (7 jobs), `Conn` (four kinds of state in one bag) |
| State expression | The close protocol is 7 fields + 5 transition functions + 26 entry points, **with no single transition function** |
| Where the spec lives | 39.4% comments, carrying the invariants; `handleWrite` spans 86 lines for 23 lines of code |
| Distribution contract | One file is the product promise and cannot be split; but the public symbol surface can shrink by ~70% |

The core judgement: **the complexity in this file is in its state, not its algorithms.** The
main landing sites across eight review rounds (case E / case F / P0-5 / R7-1 / R8-1) are all
in the close protocol, and that protocol's complexity comes from "one fact recorded
separately in seven fields". That is a structural problem, not an implementation one.

---

## 1. Measurement baseline

| Metric | Value |
|---|---|
| Total / code / comment / blank lines | 2989 / 1702 / 1106 / 181 |
| Comments as a share of non-blank lines | **39.4%** |
| Functions | 41 |
| Longest function | `start()` 169 lines (115 code lines) |
| Next longest | `acceptLoop()` 163, `parseFrames()` 114, `drainInputThenClose()` 113 |
| `Conn` fields | 23 |
| `SubReactor` methods | 32 |
| `ServerStats` counters | 22 |
| Call sites of `closeConn` / `parkClose` / `teardownClosed` | 26 (excluding declarations) |
| Distinct close-reason string literals (`const char*`) | 14 |
| Direct `epoll_ctl` calls | 10 |
| References to `closing` / `pendingBytes` / `isClosed` | 35 / 25 / 16 |

The comment ratio is the most honest measurement here: **one comment line per 2.5 lines of
code**, which says the author keeps compensating in prose for what the structure cannot
express. That is not a bad habit — it makes this file far more readable than C++ of its size
usually is — but it is a signal: the comments are doing work that types should be doing.

---

## 2. The layering

| Layer | Lines | Contents | Visibility |
|---|---|---|---|
| L0 protocol codec | 46–97 | `FRAME_MAGIC` / `writeFrameHeader` / `pongFrame` / `encodeFrameInto` | visible across `namespace industrial` |
| L1 infrastructure | 99–234 | `FdGuard`, `RingBuffer` | visible |
| L2 observability | 236–295 | `ServerStats` (22 atomics) | visible |
| L3 connection state | 297–486 | `SendResult`, `Admission`, `Conn`, `DrainOutcome`, `ClosedConn` | visible |
| L4 reactor | 488–620 + 882–2396 | `SubReactor` declaration + implementation | visible (the comment says "internal interface") |
| L5 server | 622–880 + 2398–2988 | `MultiReactorServer` | public API |

### 2.1 The L3/L4 boundary is nominal

`Conn`'s 23 fields fall into four groups by ownership:

| Group | Fields | Count | Owner |
|---|---|---|---|
| Identity | `id` `fd` `peer` `owner` | 4 | server / reactor |
| IO buffers | `rbuf` `wbuf` `writeArmed` `sendFrameRemain` | 4 | reactor thread only |
| **Close state machine** | `closing` `finSent` `deferredRound` `closeInputBytes` `drainReadBytes` `peerClosed` `closeStartedAtMs` `finSentAtMs` | **8** | reactor thread only (`closing` is the exception: read cross-thread) |
| **Scheduling + quota** | `readPending` `readNext` `readPrev` `pendingBytes` `admission` `isClosed` | **6** | reactor + cross-thread |

**8 + 6 = 14 fields (61%) are not properties of a connection at all; they are registers of
`SubReactor`'s state machine.** `Conn` is therefore neither a data object nor a behaviour
object — it is a call stack turned into a struct.

The consequence is measurable: every new `Conn` field means re-reading `SubReactor`'s five
close functions. R8-1 (clearing `closing` in `unregisterConn`) could become a real defect
precisely because `closing`'s lifetime crosses the boundary between the two classes, and no
single piece of code owns all of its write sites.

---

## 3. Dependency direction: the L4 ↔ L5 cycle

The nominal direction is L5 → L4. The reverse edge exists too:

```
MultiReactorServer ──owns──> SubReactor        (forward: reactors_)
SubReactor ──server_──────> MultiReactorServer (reverse: raw pointer + friend)
Conn ──owner──────────────> SubReactor         (L3 → L4, also reverse)
```

`SubReactor` reaches L5's private members directly via `friend class SubReactor` (L794):

| L5 private member reached | Access point |
|---|---|
| `server_->cfg_` | `submitNewConnection`, `drainWbufToSocket`, `parseFrames` (PONG quota) |
| `server_->stats_` (via `statsRef()`) | 45 sites |
| `server_->msgHandler_` / `stateHandler_` / `closeHandler_` | `parseFrames`, `flushCloseCallbacks` |
| `server_->onConnectionOpened/Closed` | registration / deregistration |

In other words: **these two classes are structurally one component, cut into two pieces for
writing.** This is not "deliberate tight coupling"; it is a missing intermediate concept — a
"reactor run context" (read-only config + counters + callbacks + connection registry) that
was never named.

Once that concept exists, the `friend` can go, L4 depends on a six-member read-only struct,
and the reactor becomes constructible in a test without a `MultiReactorServer`.

---

## 4. The close protocol: the expensive part

### 4.1 As it stands

**State variables (7 plus one mutual-exclusion flag)**

| Variable | Type | Meaning |
|---|---|---|
| `isClosed` | `atomic<bool>` | removed from both registries |
| `closing` | `atomic<bool>` | graceful close requested, new submissions refused |
| `finSent` | `bool` | `shutdown(SHUT_WR)` has returned |
| `deferredRound` | `enum{None,Close,PeerClosed}` | who owes a queue round |
| `closeInputBytes` | `int64_t` | input still owed to the peer at close time |
| `drainReadBytes` | `size_t` | bytes read this round (billing) |
| `peerClosed` | `bool` | the peer's FIN has arrived |

**Transition functions (5, calling each other)**

```
closeConn ──> parkClose ──> drainWbufToSocket
    │              └──────> tryBeginWriteShutdown
    └──────────> teardownClosed

drainInputThenClose ──> handleRead ──> finishPeerClosed ──> enqueue(itself)
                                                └────────> teardownClosed
handleWrite ──> drainWbufToSocket / finishPeerClosed
```

**Entry points (26)**: 20 in `closeConn`, 8 in `parkClose`, 10 in `teardownClosed` (they
overlap, because the first two call the third internally).

### 4.2 Why this is architectural rather than an implementation problem

Three pieces of evidence, all from the repository's own records:

1. The comment on `Conn::deferredRound` reads: "One bool cannot say which, and conflating
   them cost the peer its last answer" — **one field had to be split into two values because
   one bool cannot express two different senses of "owing a round"**. That is state modelling.
2. The comment in `drainInputThenClose` reads: "Charging `served * FRAME_HEADER_SIZE` made
   this a frame counter…" — **the budget's unit was once wrong by an order of magnitude
   (frames vs bytes)**, because the budget was a bare `int64_t` with no type protection.
3. Across eight rounds: case E (0/229432 bytes), case F (11/22 bytes), P0-5 (a half-closed
   peer reset), R7-1 (a reply queued just as close and FIN were read together, swallowed) —
   **four independent defects, one state machine**.

Defect density correlates with the number of state variables, and the data shows it: the close
protocol is about 18% of the code (~300 lines) but accounts for the P0/P1 entries of five of
the eight review rounds.

### 4.3 The suggested structure

Gather the seven fields into one sub-struct and concentrate the transitions in a pure function:

```
struct CloseState {
    Phase phase;              // Open | Closing | FinSent | Torn
    bool  peerFinSeen;
    Round round;              // None | Close | PeerClosed
    Budget inputBudget;       // strongly typed, bytes inside
    int64_t startedAtMs, finSentAtMs;
};

// pure: touches no socket, no epoll, no stats
Transition decide(const CloseState&, CloseEvent);
```

`CloseEvent` has only six values: `AppClose` / `PeerFin` / `DrainDone` / `BudgetSpent` /
`LingerExpired` / `IoError`. The 26 entry points collapse into 6 event delivery points. The
transition table can be unit-tested directly in `verify/` — **no socket, no thread, no
epoll** — because it is a pure function.

This is the single most valuable recommendation in this document: it turns a state machine
that could only be guarded by 33 configurations of a several-second integration suite into a
table that can be enumerated in milliseconds.

> When it was implemented, the table turned out to be larger than the "8 states × 6 events"
> sketched in R3 below: 4 phases × 6 events × every fact combination = 2304 cells. See
> `verify/close_state_machine.cpp`.

---

## 5. Four connection indexes and a global read lock

The same fact — "how many connections are there" — has four sources:

| Source | Location | Lock |
|---|---|---|
| `MultiReactorServer::conns_` | L874 | `connsMutex_` (`shared_mutex`) |
| `SubReactor::conns_` | L617 | none (worker-exclusive) |
| `SubReactor::fdIndex_` | L616 | none (worker-exclusive) |
| `admitted_` | L868 | `atomic<size_t>` (`Admission` RAII) |

The four must agree, kept in step by two callbacks: `onConnectionOpened` /
`onConnectionClosed`. This is **a duplicated source of truth**, and the cost is concrete:

- Every `sendTo()` call does: `isRunning()` → shared lock on `submissionMutex_` →
  `isRunning()` → shared lock on `connsMutex_` → `conns_.find()` → `shared_ptr` copy
  (atomic ++) → `pendingBytes` CAS → `submitSend` → `shared_ptr` destruction in the task
  (atomic --).
- **On the multi-producer path, every send takes two global locks and does two atomic
  refcount operations.** This is the server's only cross-thread read hotspot.
- Consistency maintained by callbacks means "forgot to call `onConnectionClosed`" is a
  silently-failing defect class (the comment on `discardRejectedConn` describes exactly this).

**Recommendation (requires benchmark validation; do not change this by reasoning alone)**:

1. Make `id → SubReactor*` the primary index (no `shared_ptr`); the reactor owns the
   connection's lifetime, and `shared_ptr` is used only for cross-thread hand-off. Then
   `sendTo` stops doing two atomic RMWs.
2. Shard the index 16 ways on `id & 15`, one `shared_mutex` per shard, removing the global
   read lock.
3. Verification: before/after with `verify/bench_send.cpp` and `verify/bench_rx.cpp`, in an
   8-producer scenario. Project rules require the numbers in the commit message; this one
   must comply.

---

## 6. The one-file contract and internal boundaries

The first paragraph of the README is this project's product definition:

> copy `include/tcp-server-mt.h` into your project. No `.cpp`, nothing to link.

So **splitting the file is not an option**. But the costs of one file are measurable, and most
of them need not be paid:

| Cost | Mitigable? | How |
|---|---|---|
| Every includer compiles ~3000 lines plus the full set of system headers | partly | offer an `INDUSTRIAL_TCP_DECL_ONLY` switch that pulls in declarations only |
| No per-layer unit testing | yes | isolate L0–L2 in a `detail` namespace; tests include the header and reach in |
| ~20 public symbols, all under `industrial::` | yes | move most L0–L3 symbols into `industrial::detail` |
| No TU boundary, no link-time isolation | no | accept it |

**The list to keep at the top level of `industrial::`**:

```
MultiReactorServer, Config, ServerStats, SendResult, FrameType,
FRAME_MAGIC, MAX_FRAME_PAYLOAD, nowMs
```

Move into `detail`: `FdGuard`, `RingBuffer`, `Admission`, `Conn`, `ClosedConn`,
`DrainOutcome`, `writeFrameHeader`, `pongFrame`, `encodeFrameInto`.

Benefit: the chance of a caller misusing an internal type drops to zero, and `Conn`'s 23
fields go from "public contract" to "implementation detail", free to be refactored.

> In the event, `encodeFrameInto` and `writeFrameHeader` stayed public — every suite builds
> frames with them, and `idle_semantics` needs the header writer for a half frame. See
> `r4-r6-record.md` §R5.

---

## 7. Other structural findings

| ID | Location | Problem | Impact | Suggestion |
|---|---|---|---|---|
| S1 | L2201 | `finishClose(shared_ptr&)` has the body `c.reset()` | an empty abstraction layer, one call site (L2214); the reader has to jump there to discover it does nothing | inline and delete |
| S2 | L450–452 | `readPending` / `readNext` / `readPrev` are intrusive list nodes hanging directly off `Conn` | scheduler state pollutes the connection object; `Conn` cannot be relocated | extract a `ReadQueue`, nodes still inline but their meaning restored |
| S3 | 26 sites | close reasons are `const char*` literals with 14 distinct values | not enumerable, not countable, not assertable in tests | `enum class CloseReason` + `toString()` |
| S4 | L237–295 | 22 counters with no snapshot or diff type | the README promises "record before/after per change" with no tool behind it | `struct StatsSnapshot` + `diff()` |
| S5 | L2469 | `start()` is 169 lines doing 8 things | the only startup path and the hardest function to change | split into `validate()` / `raiseFdLimit()` / `bind()` / `spawnReactors()` / `handshake()` |
| S6 | L2704 | `acceptLoop()` is 163 lines doing 7 things | fd setup, accept loop, admission, socket tuning, peer formatting, reactor choice, teardown | split into `setupEpoll()` / `acceptBatch()` / `formatPeer()` / `tuneSocket()` |
| S7 | L744–747 | `failWorkerForTest` / `failAcceptForTest` are public members of a production class | test hooks leak into the public API | inject a `FaultHooks` interface with a default no-op |
| S8 | L859 | `thread_local callbackServer_` + `CallbackScope` is implicit global state | `SubReactor::stop()`, `~MultiReactorServer()` and `start()` each read a raw pointer to decide "am I inside a callback" | converge on a single `Server::isInsideCallback()` |
| S9 | throughout | `noexcept` + `catch(...)` swallow exceptions; failures only reach counters | undiagnosable in production, 22 numbers and nothing else | a single `onInternalFault(code)` hook (count by default, optional logging) |
| S10 | L2499 | `start()` mutates `cfg_.maxPendingBytes` in place | configuration goes from immutable input to mutable state | a separate `ResolvedConfig` (the already-correct precedent is `resolvedReactorCount()`) |

---

## 8. Priority roadmap

Ordered by **risk reduction / cost of change**. Each item carries a verification criterion that
can be executed on Linux, following the project rule that nothing is concluded without
runtime evidence.

### R1 — typed close reasons (low risk, half a day)

`const char*` → `enum class CloseReason`. The 14 values become an enum, stringified in
`flushCloseCallbacks`.
Verify: the nine existing suites stay green; `grep -c '"' closeConn` should be 0.

### R2 — snapshot and diff (low risk, half a day)

`StatsSnapshot` + `diff()`.
Verify: add an assertion to `verify/config_budget.cpp` that `accepted - closed` from any
`diff()` equals `connectionCount()`.

### R3 — centralize the close state machine (medium risk, 2–3 days, largest payoff)

`CloseState` sub-struct + `decide()` pure function + `CloseEvent` enum.
In two steps:
1. **Extraction only** (fields move into the sub-struct, behaviour unchanged), run all 33
   configurations;
2. Then rewrite the five function bodies as "deliver an event, execute the returned action",
   one at a time.

Verify: a new `verify/close_state_machine.cpp` enumerating `decide()` exhaustively,
asserting that every cell is either a legal transition or explicitly an illegal combination.
This is the first time the project can cover the close protocol with a millisecond-scale test.

### R4 — split `start()` / `acceptLoop()` (medium risk, 1 day)

Purely mechanical, no behaviour change.
Verify: `verify/fault_injection.cpp` (startup-failure injection) + `config_budget.cpp` green.

### R5 — narrow the public surface (low risk, half a day)

A `detail` namespace.
Verify: `cmake --build` green; `verify/evidence/probe_api_contract.cpp` needs updating — the
existence of that probe is itself the acceptance criterion for this change.

### R6 — index refactor (medium-high risk, needs a benchmark, 2–3 days)

Sharding plus dropping the `shared_ptr`.
Verify: 8-producer throughput and p99 from `verify/bench_send.cpp`, before and after, with the
numbers in the commit message. **No numbers, no merge.**

---

## 9. Explicitly not recommended

| Proposal | Why not |
|---|---|
| Split into `.h` + `.cpp` or several headers | breaks the README's product promise (copy one file), which is the reason this library exists |
| Introduce an asio / libevent abstraction | the README says no third-party dependencies; the abstraction's benefit does not repay the determinism it costs |
| Turn `Conn` into a POD passed to handlers | `sendTo` depends on the direct `Conn::owner` path; the rework costs far more than it returns |
| Make `RingBuffer` a lock-free SPSC | `wbuf` is single-thread-exclusive with no second producer; no gain |
| Replace `SendResult` with `std::expected` | C++17 target, and `SendResult`'s six values are already sufficient modelling |
| Log everything | this library is positioned as "zero-dependency single file"; logging infrastructure contradicts that, and S9's hook covers it |

---

## 10. In one sentence

The code quality of this file is markedly higher than its structural quality. The comments,
the invariant statements and the single-exit discipline are all above average; what is
missing is **translating the state machine already described in prose into types**. Eight
review rounds have already paid for that with four real defects — R3 is the one change that
settles the bill.
