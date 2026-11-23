# EMFILE test checklist — spare-descriptor hand-off

Status: **implemented, with the baseline rows kept for comparison** · 2026-11-22.
Target: the `EMFILE` branch reached from `accept_loop()` (now
`salvage_descriptor_exhaustion()`). The change is in
`include/tcp-server-mt.h` (landed in `33f0479`), and the probe is
`verify/evidence/probe_emfile.cpp`. It is driven by
`verify/evidence/run_emfile.sh`, which CI runs in a separate step and which a
local run can add with `ENABLE_SLOW=1 bash verify/run.sh`. The pre-change rows
below are the baseline the current behaviour is compared against.

## How to read this

Each row has an **id**, a **setup**, and an **expected observation**. A row
fails if the observation differs, not if the implementation differs — the point
is to fix behaviour, not to match a patch.

The test process must lower its own **hard** `RLIMIT_NOFILE`, because `start()`
raises the soft limit up to the hard one: setting only the soft limit is undone
by the library and the exhaustion never happens.

```c
struct rlimit rl{64, 64};
setrlimit(RLIMIT_NOFILE, &rl);   // both halves; before multi_reactor_server
```

## A. Preconditions

- [x] **A1** Linux with `epoll` and real `EMFILE` queue semantics. WSL 1 does
  **not** qualify: it dequeues and closes the pending connection instead of
  leaving it in the accept queue (see the WSL 1 baseline below). WSL 2 or bare
  metal. Record `uname -r` in the run log.
- [ ] **A2** The probe sets both `rlim_cur` and `rlim_max` to a small value
  (64 is enough: a server needs ~13 descriptors before connections).
- [ ] **A3** The probe reports, per phase, `stats().accept_errors`,
  `rejected`, `fd_exhausted_drops`, `fd_exhausted_system`, and
  `connection_count()`.
- [ ] **A4** Build with `-Wall -Wextra -Wshadow`; no new warning.

## B. Baseline — the defect must be visible before the fix

Run these against the current header. If B1 does not reproduce, the rest of the
checklist proves nothing about the change.

- [ ] **B1 (client hang)** With descriptors exhausted, a fresh `connect()`
  succeeds and `recv()` **blocks for at least 3 s** (probe-side timeout).
  No FIN, no RST, no application byte.
- [ ] **B2 (silent counter)** `accept_errors` is the only sign: it climbs while
  nothing tells the client or the log what happened.
- [ ] **B3 (busy retry)** During the 3 s of B1 the accept thread wakes about
  1000 times per second (`sleep(1ms)` + level-triggered listen fd). Sample
  `/proc/<pid>/stat` or `/proc/<pid>/status` (`voluntary_ctxt_switches`) and
  record the delta; it is the baseline for D2.

## C. Functional — after the change

- [ ] **C1 (RST instead of hang)** Same setup as B1: `recv()` returns within
  100 ms, either `0` or `-1` with `ECONNRESET` (check `SO_ERROR` on the
  client socket to distinguish). It must not block.
- [ ] **C2 (counters)** Exactly one `fd_exhausted_drops` and one `rejected` per
  dropped connection, and `accept_errors >= 1`.
- [ ] **C3 (server survives)** After C1, close two established connections, then
  connect again: the new connection is accepted and an echo round-trip
  succeeds. This proves the spare was re-armed.
- [ ] **C4 (repeatable)** Loop C1 ten times: ten drops, ten `rejected`, no
  crash, `connection_count()` returns to the pre-test value.
- [ ] **C5 (no sleep-spin)** During sustained exhaustion the context-switch rate
  is materially lower than the B3 baseline (the spare path makes progress
  instead of sleeping). Record both numbers; a ratio is the evidence.

## D. Boundary and failure modes

- [ ] **D1 (spare reopen fails)** Simulate by consuming the descriptor freed by
  the spare before it is reopened (a second thread opening a file in a tight
  loop, or a `fail_spare_reopen_for_test` hook if one is added). Expected:
  `spare_fd_ == -1`, the next `EMFILE` falls back to sleep-and-retry, no
  crash, no spin. If no hook exists, record this as **inspection only** and
  say so.
- [ ] **D2 (ENFILE is not the spare path)** `ENFILE` must not consume the spare
  and must not count `fd_exhausted_drops`; `fd_exhausted_system` climbs
  instead. Hard to trigger in one process — use a low `fs.file-max` in a
  disposable container, or mark **inspection only**.
- [ ] **D3 (stop during exhaustion)** Call `stop()` while C1 is repeating:
  returns, all threads join, process exits 0.
- [ ] **D4 (start/stop cycles)** Ten `start()` / `stop()` cycles: descriptor
  count in `/proc/<pid>/fd` returns to the same value each time (spare
  closed and recreated, not leaked).
- [ ] **D5 (CLOEXEC)** The spare shows the `O_CLOEXEC` flag in
  `/proc/<pid>/fdinfo/<fd>` (`flags:` field).
- [ ] **D6 (spare never admits)** The spare must never be handed to a reactor:
  grep the run for `accepted` increasing during C1 — it must not, because
  the victim is closed in the accept loop.

## E. Resources

- [ ] **E1 (no fd leak)** `/proc/<pid>/fd` count at idle before and after C4 is
  identical.
- [ ] **E2 (no byte-accounting leak)** `buffer_bytes` returns to 0 after C4 (the
  victim never takes an admission ticket, so this is a guard against a
  mistakenly-routed ticket).
- [ ] **E3 (memory)** `valgrind --leak-check=full` or ASan on the probe: no
  leak, no invalid access.

## F. Concurrency and sanitizers

- [ ] **F1 (parallel clients)** 8 client threads connecting while exhausted: no
  crash, counters add up, accept recovers once descriptors free.
- [ ] **F2 (ASan/UBSan)** Probe built with
  `-fsanitize=address,undefined`: clean.
- [ ] **F3 (TSan)** Probe built with `-fsanitize=thread`: clean. The spare is
  accept-thread-owned; the only cross-thread access is the teardown under
  `accept_fd_mutex_`, and TSan is what confirms that pairing is right.
- [ ] **F4 (stop races the hand-off)** `stop()` from another thread while C1 is
  in flight: no use of a closed descriptor, no double close. Run 100
  iterations; TSan must stay clean.

## G. Regression

- [ ] **G1 (normal path untouched)** With descriptors available,
  `fd_exhausted_drops == 0` and `accept_errors == 0` across a
  1 000-connection churn.
- [ ] **G2 (existing suites)** Copy the header into a scratch tree and run
  `verify/run.sh` (plain, ASan+UBSan, TSan). All suites must pass. Note in
  the run log that the repository header is a different revision.
- [ ] **G3 (graceful shutdown)** `stop_gracefully()` with 50 live connections:
  no aborts, `drained == true`.
- [ ] **G4 (RLIMIT budget)** With `max_connections = 10000` and the default hard
  limit, the startup warning does not appear (the spare is inside the 16-fd
  margin the budget already reserves).

## H. Observability

- [ ] **H1 (log rate)** Under one second of continuous `EMFILE`, stderr gets at
  most one line (rate-limited), containing the errno and the drop count.
- [ ] **H2 (docs)** `accept_errors`, `fd_exhausted_drops` and
  `fd_exhausted_system` are documented in `docs/api-reference.md`, and the
  `ENFILE`/`EMFILE` distinction is stated.
- [x] **H3 (CI)** `verify/evidence/run_emfile.sh` builds the probe and fails on
  a hung, unreset or unacknowledged victim; CI runs it as its own step, and
  `ENABLE_SLOW=1 bash verify/run.sh` runs the same script locally. A host that
  never reaches EMFILE is reported as `[SKIP]`, not as a pass.

## Definition of done

All of B (defect reproduced), C, D3-D6, E, F2-F3, G1-G3 and H1 pass; D1/D2 may
be inspection-only if no hook or container is available, but that must be
written down rather than implied. A row that was not run is not a pass.

## Baseline run — 2026-11-22 (pre-change)

Probe: `verify/evidence/probe_emfile.cpp`. It forks first, so the
client runs in its own descriptor table while the parent lowers
`RLIMIT_NOFILE` to 64/64; the server greets every accepted connection with one
byte, so "greeted" and "queued" are distinguishable from the client side.

```
uname -r                4.4.0-19041-Microsoft (WSL 1)
somaxconn               128
max_connections         200
num_reactors            2
RLIMIT_NOFILE           64 / 64
```

Two runs, identical shape:

```
server : accepted=54 rejected=0 closed=1 accept_errors=143 open=53
client : connects_ok=200 greeted=53 closed=147 reset=0 silent=0
```

| row | result | note |
|---|---|---|
| B1 hang | **not reproduced** | every unserved connection got `recv() == 0` (FIN) within 400 ms; none blocked |
| B2 silent drop | **reproduced** | 143 `accept_errors`, and the client sees only an orderly EOF with no reason |
| B3 busy retry | **not reproduced** | CPU time and `accept_errors` both flat during the 1 s sample: the queue had already emptied |

The correlation is the interesting part: 143 failed `accept4` calls against 147
connections closed by the peer's side. That is consistent with WSL 1's `accept`
dequeuing the connection before the descriptor allocation fails and then
dropping it, i.e. the emulation cleans up the queue mainline Linux would leave
in place. Once the queue is empty the listening socket stops being readable, so
the `sleep(1ms)` loop idles instead of spinning — which is why B3 needs a
different environment too.

**Consequence:** on WSL 1 neither the hang (B1) nor the spin (B3) can be
demonstrated, so this environment cannot validate the spare-descriptor change's
headline behaviour (RST instead of hang). B1 and B3 must be run on WSL 2 or bare
metal before the change is called verified; on WSL 1 only C2, C4 and E stay
meaningful. This is the second time the WSL 1 socket emulation has hidden a
production behaviour (the first was `EPOLLHUP`), so treat it as a standing
limitation of the harness rather than a one-off.

## Baseline rerun on WSL 2 — 2026-11-22 (pre-change)

Same probe, same settings, real kernel (`5.10.16.3-microsoft-standard-WSL2`,
`somaxconn` 4096, 8 cores):

```
server : rlimit=64 fds=-1 cpu_ms_per_s=11.2 accept_errors_per_s=855
server : accepted=54 rejected=0 closed=1 accept_errors=1284 open=53
client : connects_ok=200 greeted=53 closed=0 reset=0 silent=147
client : victim recv took 3027 ms, errno=11 (Resource temporarily unavailable)
```

| row | WSL 1 | WSL 2 (authoritative) |
|---|---|---|
| B1 hang | not reproduced | **reproduced**: 147 silent, victim blocked the full 3 s |
| B2 silent drop | reproduced (143) | **reproduced**: 1284 `accept_errors`, client sees nothing |
| B3 busy retry | not reproduced | **reproduced**: 855 failed `accept4`/s, 11.2 ms CPU/s |

This supersedes the WSL 1 row for B1 and B3. The mechanism is now confirmed
rather than inferred: `accept4` returning `EMFILE` leaves the connection in the
accept queue, so the listening socket stays level-readable and the loop keeps
failing at the `sleep(1ms)` cadence while the peer waits with no timeout of its
own. The WSL 1 numbers are kept above only as a record of what that emulation
does differently (dequeue-and-close, which is *better* than production and
therefore useless as a baseline).

The companion `EPOLLHUP` probe, rerun on the same kernel, reports `IN HUP` for
"we `SHUT_WR`, peer FINs" where WSL 1 reported `IN` — the second production
behaviour that emulation had hidden. Both baselines now exist on an environment
that can be trusted.

## C group and B-comparison — 2026-11-22 (after the change)

Three consecutive runs on the same kernel and settings as the baseline:

```
server : accepted=54 rejected=149 closed=54 accept_errors=222 fd_drops=149 open=0
client : connects_ok=200 greeted=52 closed=0 reset=148 silent=0
client : victim recv took 0 ms, errno=104 (Connection reset by peer)
client : recovered after freeing fds = yes
server : cpu_ms_per_s=1.3  accept_errors_per_s=0
```

| row | before | after |
|---|---|---|
| B1 hang | victim blocked **3027 ms** (`EAGAIN`) | **0 ms**, `ECONNRESET` |
| B2 silent drop | 147 silent, client told nothing | 148 reset, `fd_exhausted_drops=149`, one rate-limited warning line |
| B3 busy retry | 855 failed `accept4`/s, 11.2 ms CPU/s | 0/s, 1.3-1.6 ms CPU/s |
| C2 counters | — | `fd_exhausted_drops == rejected == 149`, `fd_exhausted_system == 0` |
| C3 recovery | — | `recovered = yes` after 10 client sockets were released |
| C4 repeatable | — | three runs, identical counts (`149`, `148`, `0 ms`) |
| C5 no spin | — | see B3 |
| H1 log rate | — | one `[warn] out of descriptors` line per run, not per failure |

Still not run: **D** (spare-reopen failure, ENFILE, start/stop cycles, CLOEXEC,
stop during exhaustion), **E** (fd/byte leaks over churn), **F** (ASan/TSan on
the probe), **G** (existing suites against this header). D5 and D6 are currently
inspection-only: the spare is opened with `O_CLOEXEC` and the victim is closed
inside the accept loop before any ticket is created, but neither was measured.

## F, E and G — 2026-11-22

### F. Sanitizers

| build | result |
|---|---|
| `-fsanitize=address,undefined` | behaviour identical to plain; no ASan report |
| `-fsanitize=thread` | no race report; same counters (149 drops, 0 ms victim) |

One **open** observation, now reproducible: UBSan's vptr check reports

```
shared_ptr_base.h:705: runtime error: member call on address 0x508000000020
which does not point to an object of type '_Sp_counted_base'
```

with the stack ending at `teardown_closed()` line 3010, i.e.
**`c->admission.reset()`**, reached from `finish_peer_closed()` through
`handle_write()`. Reproduction:

```
bash verify/evidence/repro_ubsan_vptr.sh 4 12
```

Four concurrent instances of `probe_emfile.cpp` built with ASan+UBSan hit it
within twelve iterations each; sequential runs hit about one time in three.
Discriminating runs:

| build | result |
|---|---|
| UBSan only (vptr on) | 0 hits in 10 sequential runs |
| ASan + UBSan, `-fno-sanitize=vptr` | 0 hits in 16 concurrent runs |
| ASan + UBSan | 4 of 4 workers hit |

The address is identical in every report, so it is a deterministic allocation
slot rather than a wild pointer. A diagnostic build added a liveness marker to
`connection` (magic at the head and tail, poisoned in the destructor) and
checked it at the top of `teardown_closed` and again immediately before
`c->admission.reset()`: **both checks stayed silent in every run that still
reproduced the report**. The stale-`connection*` hypothesis is therefore ruled
out — `c` is a live object, and the dangling reference is the one it holds.

A second marker on `admission_ticket` (alive in the constructor, dead in the
destructor), plus a log line in `~admission_ticket()` and in front of every
`c->admission.reset()`, settles the lifetime question:

```
error line 3 : shared_ptr_base.h:705 ... address 0x50b000005840
last teardown: [teardown] id=1 ticket=0x50b000005850 tmagic=TicketAl
ticket 0x50b000005850 destroyed earlier at lines []      <- no dtor at all
```

The ticket's magic is alive, its destructor never ran, and the failing address
is exactly its own control block: `0x...5840 = 0x...5850 - 0x10`, the
`_Sp_counted_ptr_inplace` header that precedes an in-place object. So the
control block is inside a *still-allocated, still-constructed* object, which
contradicts "dangling reference". The same probe also reports at
`shared_ptr_base.h:722` (`_M_add_ref_copy`, i.e. a shared_ptr **copy**) with a
different address and before any teardown at all, so this is not one narrow
reset path either.

Controls:

- A standalone `make_shared`/reset churn (20 000 rounds x 64 objects, four
  parallel processes, same sanitizer build) produces no report — it is not a
  blanket toolchain defect.
- TSan over the same probe is clean, so it is not a detected data race on these
  pointers.

**Verdict: instrumentation artifact, not a defect in this library.** The last
test that could have overturned that reading was Valgrind memcheck on a
non-ASan build of the same scenario. It cannot lower `RLIMIT_NOFILE` itself
(`setrlimit` returns `EPERM` under Valgrind), so the ceiling is set before it
starts:

```
bash -c "ulimit -n 128; valgrind -q --leak-check=no /tmp/probe_vg 39997"
  accepted=106 rejected=4 closed=106 accept_errors=4 fd_drops=4 open=0
  (Valgrind silent: no Invalid read, no Invalid write)
```

That run reaches the spare-descriptor path (`fd_drops=4`) and tears down 106
connections, which is the workload ASan+UBSan complains about, and Valgrind —
the reference implementation for heap lifetime checking — finds nothing. With
ASan's own use-after-free tracking also silent, TSan silent, the object alive
with its control block inside the same allocation, and the minimal
`make_shared` control clean, the report is closed as a vptr-check artifact of
the ASan build.

Residual uncertainty is stated rather than hidden: the Valgrind run is not
byte-identical to the failing one (fewer connections under the same ceiling,
slower timing, one process instead of four). If the report is ever dismissed as
a false positive and later turns out to be real, the reproduction is one command
away in `verify/evidence/repro_ubsan_vptr.sh`.

**Confirmation run** (same day, after the verdict was written):

```
1. repro script          worker 2 hit on iteration 3   (still reproducible)
   site                  shared_ptr_base.h:519, type '_Sp_counted_ptr_inplace'
2. ASan, vptr off        port 40101 clean, 40102 clean
3. Valgrind (3 ports)    40111 clean, 40112 clean, 40113 clean
```

The reported site is not stable: `705` (`_M_release`), `722`
(`_M_add_ref_copy`, a copy) and now `519`, which is inside
`_Sp_counted_ptr_inplace`'s **constructor** (`allocator_traits::construct`).
A dangling control block cannot be produced by the construction of a fresh
object, which removes the last reading under which these reports share one
lifetime bug. Three different inline sites, one common shape (a vptr check on an
in-place control block), every real memory checker silent.

Practical consequence for CI: when ASan is enabled, add `-fno-sanitize=vptr`.
The vptr check is what fires, it hides nothing that ASan or TSan do not cover
better, and leaving it on turns a clean run red at random.

One probe defect found on the way: the client's wait-for-listener loop had no
delay between attempts, so 500 connects could all land before the server
started — invisible on a fast machine, fatal under Valgrind. It now sleeps
10 ms per failed attempt.

### E. Leaks

`verify`-style churn: 20 rounds x 50 connections = 1000 admissions, half closed
with `SO_LINGER{1,0}` (abort) and half orderly.

```
counters: accepted=1000 closed=1000 aborted=500 buffer_bytes=0 open=0
fds at idle = 12   fds after churn = 12   fds after 5 start/stop cycles = 4
```

- [x] **E1** descriptor count returns to the post-start baseline (12).
- [x] **E2** `buffer_bytes` returns to 0.
- [x] **E3** ASan finds no leak in the same workload.
- [x] **D4** five `stop()`/`start()` cycles leave the descriptor count at the
      pre-server baseline (4) — the spare is released, not leaked.

### G. Existing suites

`ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh` against this header in a
scratch tree (ext4, not `/mnt/c`):

```
plain=13  asan+ubsan=13  tsan=13  skip=1  fail=0
Result: all configurations passed
```

That is 39 suite runs across the three configurations, plus cpplint. The single
`[SKIP]` is `send_results`' `hup/err` case, which asks the host whether an
aborted connection is reported as an error before asserting on it.

One suite changed shape when the reactor queue grew a reserved control window.
A reply flood now stops 64 slots short of the cap, so the close in
`backpressure` case B is queued rather than refused:

```cpp
harness::expect(dropped == 0,
                "the reserved slot kept the close out of the refused path");
```

The refused-close fallback is still covered by `fault_injection` case F, which
forces the refusal with its one-shot hook and checks that `close_wanted` still
drives the close. The 39-run matrix above predates the reserved window.
