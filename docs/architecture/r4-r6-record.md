# R4–R6 record

Branch: `refactor/r4-r6-structure` (local, not pushed)
Base: the HEAD of `refactor/r3-close-state-machine`
Environment: WSL2 Ubuntu 22.04 / g++ 11.4.0 / 8 cores

| Item | Result |
|---|---|
| R4 split `start()` / `acceptLoop()` | merged |
| R5 narrow the public surface | merged |
| R6 sharded connection index | **no measured gain, reverted** |
| Prerequisite fix to `bench.sh` | merged |

---

## Prerequisite: `verify/bench.sh` had never produced a complete result

Building the R6 baseline exposed a crash in the summariser:

```
statistics.StatisticsError: no median for empty data
```

Cause: `bench_rx` emits `mode=same`, and the summary loop only skipped four fields —
`connections/reactors/clients/payload`. `mode` reached `float('same')` and threw, but
`groups[key][name]` had already been created as an empty list by `defaultdict` before the
exception, so `median()` died on it.

Consequence: any run emitting a `mode=` field made the summary abort right after
`ingest reactors=1`. **The script this project claims to produce numbers with had never
produced complete numbers.**

The fix is one line (add `mode` to the skip list). Same shape as two other places here — the
cpplint control file and the RST case in `harness_selftest` — **a check that cannot pass
reads exactly like a check that passed.**

---

## R4 — split `start()` and `acceptLoop()`

`start()`: 180 lines doing 8 things → `validateConfig` / `warnCoarseTick` / `clampConfig` /
`ensureFdLimit` / `spawnReactors` / `startAcceptThread` / `rollbackStart`.
`acceptLoop()`: 163 lines doing 7 things → `setupAcceptEpoll` / `acceptBatch` / `formatPeer` /
`tuneAcceptedSocket`.

Two places checked specifically:

1. The `||` chain in `validateConfig()` keeps its original order, including the two capacity
   comparisons that read `cfg_`.
2. `startAcceptThread()` returns `!failed_` only when the promise delivered a value, so a
   handshake timeout still falls through to `rollbackStart()` rather than reporting success.

Verified: 33/36, identical to baseline item by item (the three red are `harness_selftest`;
the WSL2 kernel ignores `SO_LINGER{on,0}`, reproduced on `main`). cpplint 0.

## R5 — narrow the public surface

`Conn`, `SubReactor`, `FdGuard`, `RingBuffer`, `Admission`, `ClosedConn`, `DrainOutcome`,
`CloseState` and its enums, `decide()`, `pongFrame()` all moved into `industrial::detail`.

**Two deliberately left public**: `encodeFrameInto` and `writeFrameHeader`. The reason is
empirical — moving both into detail broke the build of `idle_semantics.cpp`, which needs
`writeFrameHeader` to construct a half frame. Both are protocol codec entry points, the
"caller holding the bytes" side. The cached PONG is not, so `pongFrame` moved.

`onConnectionOpened/Closed` became private: their signatures name `detail::Conn`, and leaving
them public would put the type that had just been moved straight back on the surface.

The verification method changed. `probe_api_contract.cpp` only ever touched the public
surface, so after R5 it **compiles unchanged** — it is not the sentinel for this change. The
new `verify/evidence/probe_surface_shrank.sh` is: 13 translation units that must fail to
compile plus one control that must succeed. Injecting `using detail::Conn;` at namespace
scope turns it red, which is the evidence that it measures anything.

Verified: 33/36, cpplint 0, surface probe PASS.

---

## R6 — sharded connection index: no measured gain, reverted

### What was done

Every `sendTo()` call took two global shared locks (`submissionMutex_` + `connsMutex_`). The
connection table was split into 16 shards on `id & 15`, each with its own `shared_mutex`,
removing the global read lock.

**The `shared_ptr` stayed.** The plan had been to switch to a raw `Conn*` and save two atomic
refcount operations, but reading the code gave no basis for it: `teardownClosed()` erases the
connection on the owning reactor while `sendTo()` may already hold the pointer, and there is
no epoch or hazard-pointer mechanism in this file to bridge that window. **Trading a
benchmark number for a use-after-free that needs a particular interleaving is not a trade.**

### Measurements

Same machine, same script, `REPS=3`, about one minute apart. The control is the R5 tree, not
the older baseline from twenty minutes earlier (that machine state had drifted; see below).

| Metric | R5 | R6 | Intervals overlap? |
|---|---|---|---|
| rss per_conn_kb | 128.70 | 128.70 | same value |
| ingest 1 reactor | 288.10 MB/s | 335.20 MB/s | overlap (R5 286–442) |
| ingest 2 reactors | 1119.60 MB/s | 1020.40 MB/s | overlap (R6 903–1039) |
| ingest 4 reactors | 942.90 MB/s | 1018.10 MB/s | overlap (R5 877–957) |
| send p50 | 76.60 µs | 76.40 µs | overlap |
| send p99 | 150.30 µs | 143.30 µs | overlap (R6 136–174) |
| send mean | 79.70 µs | 80.90 µs | overlap |

Everything lands inside the noise. The three-configuration suite is 33/36 green and cpplint
is 0, so **correctness is fine — but nothing here shows it is faster.**

### Why it could not be measured (this matters more than the verdict)

`bench_send.cpp` is a **single-connection** round trip: one peer, 2000 messages. `connsMutex_`
has no contention under that load, so sharding has nothing to remove.

In other words: **the benchmark does not exercise the path R6 was about.** Not observing a
difference is not the same as there being none; it means this benchmark cannot ask the
question.

Continuing R6 requires a real multi-producer benchmark first: N threads × M connections
calling `sendTo` concurrently, measuring p99 and throughput. Until then R6 does not earn a
merge — by this project's own rule, no numbers, no merge.

One incidental observation: `sendTo()` allocates a `std::vector<uint8_t> frame` per message.
In a single-connection benchmark that cost plausibly outweighs the lock, which is another
reason the sharding effect is buried. This one is unverified — it comes from reading the
code, not from measuring it.

---

## The three agents

Defined under `.workbuddy-ai/agents/`: `r46-coding-agent` / `r46-test-agent` /
`r46-audit-agent`.

**None of the three ran.** The coding agent exited on a 429 after 13 seconds, the same reason
the earlier impl/audit pair failed. Implementation, testing and audit were all done by the
main executor.

That is a quota conclusion about this session, not a conclusion about agents. The definitions
are kept because the briefs inside them — acceptance criteria per step, the known
environmental red, the verification commands — are the reusable part.
