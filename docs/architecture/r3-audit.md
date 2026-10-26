# R3 audit record

Branch: `refactor/r3-close-state-machine`
Audited: the diff of `include/tcp-server-mt.h` against `main` (+344 / −104)
Criteria: `docs/architecture/r3-baseline.md` sections 1, 2, 5 and 6

The auditor and the implementer were the same process (both delegated agents produced
nothing, rate-limited), so **this record is not an independent audit**. Its value is that
every check is written as a command that can be re-run, rather than as the sentence "looks
equivalent".

---

## E1 field references — PASS

| Field | `c->close.*` lines in the new code | baseline | where the difference comes from |
|---|---|---|---|
| `closing` | 10 | 10 | — |
| `finSent` | 8 | 8 | — |
| `round` | 11 | 7 | +4: one read-only facts block each in `parkClose` / `finishPeerClosed` / `drainInputThenClose` / `handleWrite` |
| `inputBudget` | 5 | 4 | +1: the read-only `facts.budgetLeft` in `drainInputThenClose` |
| `drainReadBytes` | 4 | 4 | — |
| `peerFinSeen` | 2 | 2 | — |
| `startedAtMs` | 3 | 3 | — |
| `finSentAtMs` | 1 | 1 | — |

All five increments are reads; no new write sites. The old names (`closeInputBytes` /
`closeStartedAtMs` / `deferredRound` / `peerClosed` / `DeferredRound`) leave zero residue
anywhere in the file.

```
grep -c "c->close.round" include/tcp-server-mt.h          # 11
grep -n "closeInputBytes\|closeStartedAtMs\|deferredRound\|peerClosed\|DeferredRound" include/tcp-server-mt.h
```

## E2 atomicity — PASS

`closing` (L389) and `isClosed` (L571) are still `std::atomic<bool>`. All ten accesses to
`closing` carry `acquire`/`release`; no relaxed access slipped in.

## E3 monotonicity — PASS

The write sites match baseline section 1 line for line:

| Field | Write site | Count |
|---|---|---|
| `peerFinSeen` | L1615 | 1 (monotonic) |
| `finSent` | L1764 | 1 (monotonic) |
| `round` | L1865 / L2316 / L2350 | 3 (`None` → `Close`/`PeerClosed`, never back) |
| `closing` | L2171 (true) / L2051 (false) | 2 (the false one is still only in `unregisterConn`) |

No new reset points.

## E4 the decision tables — PASS

The three riskiest rewrites, checked one by one:

**1. `parkClose` entry gate**

Old: `if (isClosed || finSent) return;`
New: `if (decide(phase(), AppClose, facts) == None) return;`

`AppClose` returns `None` only for `FinSent` / `Torn`; for `Open` / `Closing` it always
returns `SendFin` or `Park`. Equivalent.

**2. `parkClose` after the drain**

Old: `if (!wbuf.empty()) armWrite(); else tryBeginWriteShutdown();`
New: dispatch on `decide(AppClose, wbufEmpty)` — `SendFin` → `tryBeginWriteShutdown`,
`Park` → `armWrite`.

Outside `FinSent`/`Torn`, `AppClose` can only return those two values, and they correspond
one-to-one with `wbufEmpty`. The `switch`'s `default` branch is semantically unreachable and
is kept only to avoid an unhandled-enum warning. Equivalent.

**3. `handleWrite` tail**

Old: `if (closing && !tryBeginWriteShutdown(c)) teardown;`
New: `if (decide(DrainDone) == SendFin) { if (!tryBeginWriteShutdown(c)) teardown; }`

`decide(DrainDone)` returns `SendFin` when the phase is `Closing` and `wbufEmpty` is true,
and `wbuf` is necessarily empty here (the non-empty branch returned above), so the outer
condition reduces to `phase == Closing` — the same test the old `closing` performed.
Short-circuit order is also unchanged: phase first, then the `shutdown` return value.
Equivalent.

**The four outcomes of `finishPeerClosed`**, cell by cell (the empty-wbuf / `round != None`
cell is the one the old code was easiest to misread):

| Phase | wbuf | round | pending | Old behaviour | New table |
|---|---|---|---|---|---|
| Open | empty | None | yes | defer one round | DeferPark |
| Open | empty | None | no | graceful | TeardownGraceful |
| Open | empty | **not None** | any | graceful | TeardownGraceful |
| Closing | empty | any | any | tryBeginWriteShutdown | SendFin |
| FinSent | empty | any | any | graceful | TeardownGraceful |
| any | non-empty | any | any | park | Park |

They agree.

## E5 `doTick` linger — PASS

Still reads `c->close.startedAtMs` (L2421–2422), pairing with the write at L2173 in
`parkClose`. `finSentAtMs` still has no reader (baseline already marked it a dead field;
untouched here).

## E6 callback deferral — PASS

`teardownClosed` still only pushes a `ClosedConn` onto `pendingCloses_`; no new synchronous
callback invocation.

## E7 no new allocation or locks — PASS

Lines added by the diff matching `make_shared|new |mutex|lock_guard|unique_lock|shared_lock`:
**0**.

## E8 Linux build and integration — PASS (one environmental failure, ruled out against main)

Environment: WSL2 Ubuntu 22.04, `g++ 11.4.0`, 8 cores,
`ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh`.

| Item | Result |
|---|---|
| `-Wall -Wextra -Werror` build of `verify/close_state_machine.cpp` | passes, zero warnings |
| cpplint (`include/tcp-server-mt.h`) | **0 findings** |
| plain configuration | 11/12 green |
| asan+ubsan configuration | 11/12 green |
| tsan configuration | 11/12 green |
| total | **33/36** |

**The only failure is `harness_selftest`, once per configuration.**

It fails in case D, `expectClose(Rst)`:

```
[FAIL] RST was reported: expected RST, saw FIN
[FAIL] and it is not reported as a FIN
```

This is not caused by the change. Other suites in the same log say `[SKIP]` about the same
thing:

```
[SKIP] no wire RST is observable on this kernel: it ignores SO_LINGER{on,0}
       (checked against a bare socket pair), so case C can only assert the
       server's own evidence here
```

The WSL2 kernel does not turn `SO_LINGER{on,0}` into an observable RST, so the harness's own
RST detector cannot be verified. `harness_selftest` does not probe for that first — it
asserts directly, which is why it is red.

Control experiment (the decisive evidence): build and run the same case on `main`
(`6d1eedd`, before the refactor):

```
$ g++ -std=c++17 -Wall -Wextra -Iinclude verify/harness_selftest.cpp -o /tmp/hs_main
$ /tmp/hs_main
D. expectClose(Rst) recognises a reset
  [PASS] peer connected
  [FAIL] RST was reported: expected RST, saw FIN
  [FAIL] and it is not reported as a FIN
```

**Verbatim the same.** Unrelated to this refactor; an environment limitation.

### Two incidental findings (outside R3, left alone)

1. `harness_selftest` lacks the kernel-capability probe the other suites have, so on this
   kernel it fails hard instead of emitting `[SKIP]`. By this project's own standard, a check
   should ask the machine before asserting.
2. `run.sh`'s exit status was unreliable in this run (`Result: FAILURES` while the shell got
   0). The conclusions above were drawn by reading each suite's `rc=` line, not the script's
   exit status.

### Line endings (environment)

The working tree is CRLF (`core.autocrlf=true`, no `.gitattributes` in the repo), so
`set -euo pipefail\r` makes bash report `invalid option name`. Verification was done on an LF
copy in `/tmp` with identical content. **This only affects running the scripts in WSL straight
from a Windows checkout**; it does not affect CI.

---

## Three problems this refactor introduced, and how each was caught

These were not pre-existing defects.

### A1 `decide(DrainDone)` did not check `wbufEmpty` (blocking, fixed)

The first version tested the phase only: `Closing → SendFin`. The hand-written assertion
`no FIN over a non-empty wbuf (DrainDone)` failed.

The exhaustive test did not catch it, because **the oracle is a second transcription of the
same table** and both missed the same guard. What caught it was the independently written
safety property. That is the blind spot of a table-against-table test, and it is worth
recording.

Fix: `decide` folds `!f.wbufEmpty` into the condition and returns `None` instead of
`SendFin`. The oracle gained the same guard, with a comment noting that this point is
stricter than baseline section 5 and is part of the contract.

### A2 / A3 `facts.round` not updated after a failed defer (non-blocking, fixed)

In `finishPeerClosed` and `drainInputThenClose`, the defer branch sets `round` before calling
`enqueue`; when `enqueue` fails the function continues, but `facts.round` still holds the
entry value, so a later `expectClose` reports a spurious `closeStateMismatch`.

In integration this shows up as "the counter is mysteriously non-zero when the queue is full",
which is very hard to trace. Fix: `facts.round = c->close.round;` after the assignment.

---

## Conclusion

E1–E8 pass. 33/36 green; the only red reproduces verbatim on `main` and is a kernel
capability problem rather than a code problem.

Equivalence at the code level holds, and runtime equivalence has integration evidence:
under plain / asan+ubsan / tsan, `close_semantics` (including `CS_REACTORS=1`),
`idle_semantics`, `ping_pong`, `send_results`, `backpressure`, `fault_injection`,
`reactor_spread` (both reactor counts) and `config_budget` all pass — every close-protocol
scene the eight review rounds landed on still holds after the refactor.

**The evidence for `closeStateMismatch`**: section D of `close_semantics.cpp` gained an
assertion that it is `0` after the run. It has to live there rather than in
`close_state_machine.cpp`, because only a real run reaches `expectClose()`.

That assertion was verified negatively: changing one `expectClose(..., Park, ...)` in
`finishPeerClosed` to `SendFin` turns the same suite red immediately:

```
[FAIL] decide() and the close branches agreed on every transition
FAILURES (1 failure)
```

Restoring it gives `ALL PASS (0 failures)`. So the assertion is not decoration that is always
true.
