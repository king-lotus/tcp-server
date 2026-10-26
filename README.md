# tcp-server

A Linux TCP server as a single header: C++17, level-triggered epoll, one accept
thread with N sub-reactors. Everything is in that one file.

```cpp
#include "tcp-server-mt.h"          // -I<repo>/include  -std=c++17  -pthread
industrial::MultiReactorServer server(cfg);
```

No `.cpp`, nothing to link, no third-party dependencies. To use it, copy
`include/tcp-server-mt.h` into your project. It needs Linux (`epoll` +
`eventfd`), so the machine that can compile it is also the machine that has to
run it -- `verify/` is that machine.

## Shape of it

- **One accept thread plus N sub-reactors**, each reactor running on its own
  thread and owning its own connections. A submission aimed at another thread
  travels through that reactor's queue, plus an `eventfd` wakeup.
- **Closing is staged, not `close(fd)`.** A connection is marked `closing`
  first, drains the replies already queued, and only then FINs. That semantic is
  where eight rounds of review landed most often and where the probes are
  densest.
- **The counters live in `ServerStats`**, each annotated with when it moves and
  with the branches that should reach it but cannot. Reading those comments
  beats reading this summary.

## Verifying

This project has no "just run it and see" position. Everything is settled by
building and running it on Linux:

```bash
# Nine suites, eleven runs per configuration -- two of them run twice, at
# different reactor counts -- over {plain, asan+ubsan, tsan}: 33 configurations,
# cpplint included.
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh
```

Each configuration is a separate build: ASan and TSan cannot be linked into the
same binary, and a run under either is slow enough that folding it into the
default pass would stop people running it.

Nothing here is checkable by a compiler alone. The racing, sanitizer and
semantic cases inside `verify/run.sh` are the only way this file's runtime
behaviour gets asserted -- which is why the suites compile it everywhere, not
just the trivial translation unit.

CI runs the same suite on every push and pull request -- see
`.github/workflows/c-cpp.yml`. How a suite is written, and what the client side
of one now shares, is in [`docs/testing.md`](docs/testing.md).

`CMakeLists.txt` builds the same sources for a build system or an IDE. It is a
convenience, not a second definition of "verified": `run.sh` remains the gate,
because the three configurations, cpplint and the ordering live there.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure     # the same ten runs
```

`-DTCP_SANITIZE=address,undefined` (or `thread`) rebuilds the suites under one
sanitizer set; the tsan configuration wraps every test in `setarch -R` for the
same reason `run.sh` does. `-DTCP_BUILD_BENCH=ON` adds the four benchmarks.

`verify/bench.sh` is the third kind of check, and it is deliberately not in CI:
it measures resident memory per idle connection, ingest rate at one, two and
four reactors, and the echo round trip at p50 and p99. A shared runner hands its
cores to other tenants, so a threshold there would fail on someone else's load
rather than on a regression. The numbers are worth having when they are compared
against numbers taken on the same machine minutes earlier -- which is why none
of them are written down here. Each change that moves them records the before
and after in its commit message, where the date and the machine are attached.

## Performance

Three changes in the ingest path took this from about 1.3 to 2.8 GB/s on the
development VM, and the same run from 1.42 to 0.83 cores of the eight available.
Faster and cheaper at once is what finding wasted work on the critical path
looks like; an optimisation of the algorithm would have kept the cores.

- **The idle clock was read once per frame.** `parseFrames` stamped
  `lastRecvMs`, and `handleRead` already stamps it when the bytes arrive.
  `perf` put `clock_gettime` at 18% of the reactor and `parseFrames` at 62%;
  removing the per-frame read was worth 62% of throughput.
- **The frame counter was a shared atomic, once per frame.** It is now a
  per-reactor count published once per loop iteration, `alignas(64)`. The
  alignment is the difference between +18% and -32%: a counter sharing a cache
  line with the reactor's other hot fields bounces exactly like the atomic it
  replaced.
- **`recv()` fills `rbuf` in place.** The 16KB stack buffer and the copy
  through it are gone; the ring hands out its contiguous writable span. At the
  end of the ring a read is offered only the shorter contiguous piece, down to
  one byte, and the next read continues at the wrapped tail -- never split
  across the wrap.

Two things about how these were found are worth keeping.

A measurement outlives its conditions. The counter and the in-place read were
both measured *before* the clock fix, and came out neutral and 44% worse.
Re-measured afterwards they were +18% and +9.8%. A verdict recorded against code
that has since changed is not evidence, and it will be wrong in either direction.

`perf` decided which of them was worth doing. Guessing cost four rejected
attempts -- frame budget, read size, a `Spans` struct more expensive than the
memcpy it replaced, and a header fast path worth 0.6%. One profile named
`clock_gettime` immediately.

At 4KB batches the server now reaches what `verify/bench_floor.cpp` -- the same
socket shape with nothing in between -- manages with the same client, so the read
path is no longer where the remaining difference lives. At 2.8 GB/s it uses 0.83
of 8 cores: it is waiting, not computing. What is left is in the kernel and in
the generator. `__memmove` still accounts for 20% of a profile whose total keeps
shrinking; that one is open.

## Layout

```
include/tcp-server-mt.h   the only deliverable, 2988 lines
verify/                   runtime suites, the peer harness, one-off probes
docs/testing.md           how a suite is written, and how the harness is tested
docs/review/              eight rounds of review, with an index
CPPLINT.cfg               80 columns; nothing left in it but the copyright filter
```

Those twenty probes under `verify/evidence/` are not a standing test suite. Each
one was written to settle a single claim, against a history that has since been
fixed. Two of them read the header out of git at a given revision, so they accept
either layout -- before or after the directory move on 2026-10-18.

## Still open

Three rounds (the sixth, the seventh and the eighth) each closed carrying the
same few items they could not settle themselves. They were never disproved; they
were only never executed locally. Two of the three have since been closed:

- **`EPOLLHUP` / `EPOLLERR` -- closed, 2026-10-18, on a real kernel.** This case
  printed `[SKIP]` on every machine here, because those kernels hand an aborted
  connection over as an ordinary FIN and the events never arrive; asserting
  against a break the host cannot produce would be asserting against the host.
  CI put it somewhere that can: on the runner, `send_results` reports hup/err as
  having run and asserted under plain, asan+ubsan and tsan alike, with no other
  case in the suite declining. The verdict comes back as an annotation filed by
  the step named "what this kernel was willing to tell us", not as an exit code,
  because a `[SKIP]` leaves the exit code alone: nothing else in the output
  separates "asserted and passed" from "never ran".

- **Which reactor a connection lands on -- closed, 2026-10-18.** The note here
  used to say that reactor-count-dependent behaviour had only been run with
  `numReactors = 1`, which was no longer true and had not been for some rounds:
  `close_semantics` has defaulted to two since the seventh, and the runner also
  takes it down to one on purpose, because with a single worker a fault that
  belongs to one worker is averaged away and reads as a pass. What was really
  unasserted was narrower and worse -- nothing observed `pickReactor()`'s
  round-robin, so returning the first reactor every time left all 21
  configurations green. `verify/reactor_spread.cpp` asks it directly: eight
  connections over four reactors have to be handled by four distinct threads. It
  runs at four reactors and again at two, so the assertion is shown to follow the
  configuration rather than a constant, and both existing two-reactor suites were
  run against a deliberately broken `pickReactor()` first to confirm they could
  not have caught it.

One remains open:

- **One count folded into another**: a certain class of residual drops now lands
  in `queueDrops` / `bytesDropped`, counted the same as a genuine close branch.
  Under the current contract that is the same event; separating them would take a
  new counter.

## Review

This header went through five full review rounds, plus three spent verifying
claims raised from outside. Every defect had a probe written to make it fail
first, then a fix, then that same probe run green again. The records and their
index are in [`docs/review/README.md`](docs/review/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
