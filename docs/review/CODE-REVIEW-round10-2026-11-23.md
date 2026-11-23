# Round 10: four items from outside, and one housekeeping claim that did not survive checking

- **Baseline**: `9d18ae6` (`refactor/close-fsm`, `include/tcp-server-mt.h`, 4931 lines).
  Rounds 1–9 were written against `main`; this one is the first against the
  branch that carries the per-reactor stats, the stall watchdog and the read
  pause, so the line numbers and the code being reviewed are the branch's.
- **Date**: 2026-11-23.
- **Kind**: four items raised from outside, plus one housekeeping claim. Not a
  full pass: nothing was scanned beyond what the items named and what those
  fixes touched.
- **Evidence**: WSL2 Ubuntu 22.04, g++ 11.4.0, 8 cores. `bash verify/run.sh`
  (13 attempts, one configuration) was green at `9d18ae6` before any change and
  green again after all of them. Two new probes in `verify/evidence/` and two
  extended cases were run individually, and the routing case was also run
  against a header with the fix removed.

| # | Item | Verdict | Status |
|---|---|---|---|
| R10-1 | The watchdog only logs and counts; `pick_reactor()` keeps sending new connections to a reactor that has stopped making progress | **Confirmed.** A blocked handler leaves `running_` and `dead_` untouched, so round-robin keeps handing that reactor connections nobody will read | Fixed + tested |
| R10-2 | The test hooks are public members in production builds | **Confirmed**, and the ODR fix it was introduced for was never the whole problem | Fixed + compile-checked |
| R10-3 | The setters accept a call while the server is running | **Confirmed.** A store into a `std::function` that every worker reads unsynchronized | Fixed + tested |
| R10-4 | The close-while-paused semantic is undocumented: a close answers only what is in `rbuf` | **Confirmed as undocumented.** The measurement is narrower than the claim: the budget covers `rbuf` only, but the drain's own round is exempt from the output watermark and serves one read budget of the kernel backlog; the ordinary continuation after it is the part the watermark can hold back | Documented + measured |
| R10-5 | The file again has CRLF endings and no trailing newline | **Not reproduced in the repository.** All 75 tracked files are stored with LF and end with a newline. The CRLF is this Windows checkout (`core.autocrlf=true`), and it does break `verify/run.sh` there | Corrected at the checkout level |

---

## R10-1 (P2): a stalled reactor is not routed around

### What the code did

`check_reactor_stalls()` (`:4432` at the baseline, `:4609` now) sampled
`reactor_slot::last_progress_ms` once per accept-loop pass and, on a late
sample, incremented `reactor_stalls` and printed a line. `pick_reactor()`
(`:4354` at the baseline, `:4500` now) skipped `dead_` and nothing else.

A reactor blocked inside a message handler is neither stopped nor dead:
`running_` is still true (its loop is *inside* an iteration, not past it), and
`dead_` is only set when the loop *exits*. So the watchdog's observation had no
effect on the one decision that could have acted on it. Every round-robin turn
that landed on the blocked reactor put a fresh connection into a queue whose
consumer was parked in application code:

- if the queue had room: the peer held a live TCP connection that answered
  nothing until the handler returned — indistinguishable, from the outside, from
  a server that had stopped responding;
- if the queue was full: `submit_new_connection` refused, and the peer was
  accepted and immediately closed as `rejected`, for a fault that belonged to
  one worker.

### Why skipping, not quarantining

The dead path resets a reactor's connections (`cleanup("reactor failure")`)
because that reactor will never run again. A stalled one will: it is usually a
slow handler, and destroying every connection it owns would turn a temporary
stall into a mass disconnect — the opposite of the resilience the quarantine
machinery was added for. The connection already assigned to that reactor has to
wait; a *new* one does not.

### The fix

`reactor_slot::stall_reported` became `stalled` plus `stalled_at_ms`
(`:148–172`). The watchdog sets `stalled` on the first late sample (and counts
the episode, and logs once), and clears it under hysteresis
(`check_reactor_stalls()`, `:4609`):

- **enter** at `now - last > timeout`;
- **leave** only when the reactor looks fresh by half a window
  (`now - last <= timeout / 2`) **and** a full window has passed since the mark
  (`now - stalled_at >= timeout`).

Both halves of the exit matter. Entry needs one late sample, which is what the
timeout already means. Exit without the dwell would re-admit the reactor on the
first sample after the block ends, then drop it again on the next block — a
routing flap, with a log line and a counter increment each time. The cost is
stated rather than hidden: a reactor that recovers immediately after being
flagged takes up to one timeout longer to receive new connections again.

`pick_reactor()` (`:4500`) now returns the first live, non-stalled reactor and,
when it had to pass over a stalled one, counts `reactor_stall_skips`. If every
live reactor is stalled it returns one of them anyway: a queue that drains late
still beats refusing every connection, and "every reactor is blocked in a
handler" is exactly what an overloaded server looks like. `stalled` is also
exported in `reactor_stats_view`, so the state is readable rather than inferred
from `last_progress_ms`.

### Evidence

`verify/fault_injection.cpp` case J now blocks one of two reactors in a handler,
opens six connections while it is blocked, and asserts the stalled slot keeps
exactly its one connection while the live slot takes all six, with
`reactor_stall_skips >= 1`. It then releases the handler and asserts the flag
clears and `reactor_stalls` stays at 1 — the hysteresis half.

Against a header with the skip removed, the same case fails at
`new connections are routed to the live reactor` (the round robin splits them
3/3, so three peers sit behind a parked loop). With the fix: all green.

## R10-2 (P3): the test hooks are public

The switches were made unconditional to remove an ODR hazard, and that part is
right: `#ifdef INDUSTRIAL_TCP_TESTING` around members means two translation
units can disagree about the class definition, which is undefined behaviour and
shows up as a missing symbol or a wrong offset far from the flag that caused it.

What did not follow from it is *public*. Any code holding a server reference
could write `server.fail_worker_reactor_for_test.store(idx)` in a production
build and kill a chosen reactor — a test seam that is also a foot-gun, in the
one build where nobody is watching for it.

### The fix

The switches moved into the private section, still unconditional, still checked
in every build, with `friend struct test_access;` (`:1716–1753`). The accessor
lives in `verify/harness.h`, is the only thing that names the members, and wraps
each one in a named one-shot call (`fail_worker_once`, `fail_reactor_start_after`,
`force_reactor_count`, …) so a suite cannot leave a switch armed by accident.

`INDUSTRIAL_TCP_TESTING` is gone: nothing varies with it any more, so it was
removed from `CMakeLists.txt`, `verify/run.sh`, `verify/evidence/run_one.sh` and
the docs. A define that unlocks nothing is the same kind of debt as a switch
nobody flips.

### Evidence

`verify/evidence/probe_test_hooks_private.sh` compiles two snippets: a direct
write to a switch (must fail, and must fail with `private` in the diagnostic),
and a call through `industrial::test_access` (must compile). One without the
other proves nothing — the first alone would still pass if the accessor were
broken, the second alone would pass if the members were public again.

## R10-3 (P2): the setters were unenforced

`set_message_handler` and its two siblings assigned a `std::function` with no
check of the lifecycle, on the documented assumption ("install before start;
never replace them while running") that nobody would call them late. Workers
read those `std::function`s on every frame with no synchronization, so a store
underneath them is a data race on the object's internal state, not a
reconfiguration — the failure mode is a torn pointer or a double free, not a
missing callback.

### The fix

All three now return `bool` and route through a private `install_handler()`
(`:1680`) which takes `lifecycle_mutex_` with `try_to_lock` and refuses unless
the lifecycle is `stopped`, counting `handler_installs_refused`. `false` means
the handler was **not** installed; the old one is still the one running.

Two details that matter more than the check itself:

- **`try_to_lock`, not `lock`.** A setter that races `start()`/`stop()` is
  refused rather than queued. It is also the only way a setter called from a
  callback cannot deadlock: `stop()` holds this mutex while joining the very
  thread the callback runs on.
- **The same mutex `start()` takes.** A bare `life_` read followed by an
  assignment could interleave with `start()`'s first store; holding the mutex
  makes the check and the assignment one step.

Returning `bool` rather than aborting is a judgement call: the contract is a
programming error, but this library has no exception path and terminating a
production server because of a late install is worse than refusing it. It is
counted, so the mistake is visible in `stats()` even when the return value is
ignored.

### Evidence

`verify/hardening.cpp` case F: install before `start()` returns true; all three
installers return false while running and the refusal count reaches 3; a client
still gets the *original* handler's reply; after `stop()`, an install returns
true and the restarted server answers with the new handler.

## R10-4 (P3): what a close answers while reads are paused

The item is right that the semantic was undocumented, and the header now states
it at `drain_input_then_close()` and in `close_fsm::on_request()`, plus
`docs/api-reference.md` (`close_connection`), `docs/close-fsm.md` and
`docs/architecture-zh.md` §6.1.

The measurement disagrees with the item's phrasing in one direction worth
recording. "A close answers only the requests already in `rbuf`" is a **floor**,
not a ceiling:

- `close_input_bytes` is measured once, from `rbuf`, when the request is
  dequeued — that is the part guaranteed;
- the drain resumes reads, so its own round pulls up to one read budget
  (64 KiB / 256 frames) out of the kernel;
- the connection's ordinary read continuation runs *before* the queued park task,
  so a kernel backlog can be served in full when the output side keeps up.

What bounds the extra work is the read budget and the moment the park lands, not
the close budget. Output backpressure decides whether the ordinary continuation
gets to run before that. Measured with
`verify/evidence/probe_close_while_paused.cpp` (one reactor, 4000 requests
waiting in the kernel of a read-paused connection):

```
sent=4000 answers=256 big=1 fin=1 rst=0
frames_recv: before close=1 after the close round=4001 after=4001
frames_dropped_on_close=0 bytes_dropped=2808 aborted=0 read_pauses=2
```

4000 parsed and handed to the handler, 256 replies on the wire, then FIN. The
256 is exactly the drain round: one read budget, served while `closing` was
still false. The rest were parsed by the continuation after the park had set
`closing`, so their replies were refused. The gap between those two numbers is
the finding: `frames_recv` counts work taken in, the peer counts answers
received, and only the second is a promise.

An earlier run of the same probe, before the peer's receive buffer was pinned
small enough that the 64 KiB reply could not drain, answered **4000 of 4000**.
The budget was 0 in both runs. What decided the difference was the output side:
when the reply drained, the pause cleared, the read path served the whole
backlog before the close parked, and every answer got out.

The design reason for keeping the budget denominated in `rbuf` is now written
down rather than implied: counting the kernel buffer means `FIONREAD` per close,
and draining it makes the close wait on a pipe the peer controls with only
`close_linger_ms` as a deadline — a local decision turned into a remote one.

## R10-5: the housekeeping claim, checked

The claim was "the file again has CRLF endings and no trailing newline". Checked
against the repository, not the checkout:

- all **75** blobs in `HEAD` contain no `\r` and end with `\n`, including
  `include/tcp-server-mt.h` (246,070 bytes, LF);
- the working tree of this Windows clone had CRLF in all 75 files, because
  `core.autocrlf=true` and no `.gitattributes` was present.

So the file in the repository did not need converting, and adding a trailing
newline would have been a no-op. The checkout did need fixing, and it had a
consequence worth more than cosmetics: **`bash verify/run.sh` fails on this
checkout** — `set -euo pipefail` arrives as `pipefail\r`, and bash reports
`set: pipefail: invalid option name` before running a single test. That is how
the CRLF was found.

Fix: `.gitattributes` with `* text=auto eol=lf`, and the working tree converted
back to LF. The blobs were already LF, so nothing changed in the index; what
changes is that the next checkout cannot reintroduce CRLF.

---

## What was run

| Step | Result |
|---|---|
| `bash verify/run.sh` at `9d18ae6` | 13/13 attempts green (baseline) |
| `bash verify/run.sh` after all changes | 13/13 attempts green, cpplint clean |
| `verify/fault_injection.cpp` (J: routing + hysteresis) | green; fails with the routing skip removed |
| `verify/fault_injection.cpp` (K: quarantine clears `stalled`) | green; the new assertion fails with the clear removed |
| `verify/hardening.cpp` (F: installer refusal) | green |
| `verify/evidence/probe_test_hooks_private.sh` | direct write refused as `private`; accessor compiles |
| `verify/evidence/probe_close_while_paused.cpp` | 256 of 4000 answered, FIN, `aborted` 0 |

## Left alone, and why

- **A stalled reactor's existing connections still wait.** Moving them would
  mean stealing another reactor's queue capacity for connections it did not
  accept, and re-registering descriptors across threads. Skipping new work is
  the cheap half; the expensive half needs its own design.
- **`idle_timeout_ms` still does not fire on a stalled reactor**, because the
  sweep runs on that reactor's own tick. The connection is reclaimed when the
  reactor resumes or when the server stops; making the watchdog close them would
  turn an observation into a second, differently-timed teardown path.
- **`frames_dropped_on_close` stayed at 0 while 3744 replies never reached the
  wire** in the close-while-paused probe. Only part of that gap went through the
  counted refusal path: `bytes_dropped` moved by 2808, which is 312 one-byte
  frames at 9 bytes each. The rest were turned away by `send_to` returning
  `closed` once `closing` was set, which increments no counter. Whether that
  path should be counted is a separate question; this round did not change
  counting.
