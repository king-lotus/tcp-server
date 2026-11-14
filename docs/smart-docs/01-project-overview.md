# Project Overview

## What is tcp-server?

`tcp-server` is a TCP server library for Linux shipped as a **single C++17
header**. An application includes `include/tcp-server-mt.h`, constructs one
`multi_reactor_server`, installs a message handler, and calls `start()`. There
is no `.cpp` to compile, nothing to link, and no third-party dependency — the
header is the deliverable, and copying it into a project is the installation
step.

It is a library, not a program: the repository contains no production `main()`.
The entry point is whatever application embeds it, and the repository exists to
develop and prove the header itself.

## Core purpose

Serve many long-lived TCP connections from a small number of threads, with
predictable behaviour when a peer misbehaves:

- **Bound the cost of a bad peer.** Idle timeouts, a separate deadline for
  half-assembled frames, a maximum frame size, per-connection backpressure and
  an admission ceiling each cap a different resource.
- **Make closing a state machine, not a `close()` call.** A connection drains
  what it owes the peer, sends FIN, and waits a bounded time for the peer's FIN
  before it is torn down.
- **Keep the reactor free of slow work** without reordering a connection's
  messages: an optional worker pool runs tasks serially per connection.
- **Make every loss countable.** Nothing is dropped silently; each counter in
  `server_stats` names the branch that moves it.

## Technology stack

| Layer | Choice |
|---|---|
| Language | C++17, standard library only |
| I/O model | Level-triggered `epoll`, one `eventfd` per reactor for cross-thread wakeups |
| Threads | `std::thread`; one accept thread, N reactors (≤ 64), optional handler pool (≤ 64) |
| Synchronisation | `std::atomic`, `std::shared_mutex` sharded over 16 connection shards, one mutex per reactor queue |
| Syscalls relied on | `epoll_create1`, `epoll_ctl`, `epoll_wait`, `eventfd`, `accept4`, `recv`, `sendmsg` with `MSG_NOSIGNAL`, `shutdown`, `setsockopt`, `getrlimit`/`setrlimit` |
| Build | `CMakeLists.txt` (INTERFACE target + tests), plus `verify/run.sh` as the authoritative gate |
| Test toolchains | Plain build, ASan + UBSan, TSan; `cpplint` at 80 columns |
| CI | GitHub Actions, three configurations, on every push and pull request |
| Platform | Linux only — the header will not compile elsewhere, by design |
| License | Apache License 2.0 |

## Key features

- **N reactors, one accept thread.** Connections are spread round-robin over
  reactors, each owning its own descriptors and buffers.
- **Framed protocol with heartbeats.** An 8-byte big-endian header (magic, type,
  length) carrying up to 64 KiB; PING is answered by the library, PONG is
  consumed, everything else is delivered to the application.
- **Owner-thread fast path.** A reply produced inside a message handler writes
  straight into the write buffer, bypassing the queue, the locks and the
  wakeup; the same call from any other thread is queued.
- **Staged graceful close**, with a bounded linger for the peer's FIN, and
  RST-based abort for protocol violations.
- **Backpressure**, not unbounded buffering: a connection that stops reading
  hits `max_pending_bytes` and further frames are refused with a result code.
- **Optional offload pool** for work that must not block a reactor.
- **Observability by default**: 20 atomic counters, each documented with the
  branch that moves it.
- **Operational honesty**: `start()` refuses an impossible configuration, tries
  to raise `RLIMIT_NOFILE`, and warns when the machine cannot serve the
  configured connection count.

## Project structure

```
tcp-server/
├── include/
│   └── tcp-server-mt.h          the single deliverable (3,445 lines)
├── verify/
│   ├── harness.h                shared peer-side client for the suites
│   ├── *.cpp                    9 standing suites + 4 benchmarks
│   └── evidence/                15 one-off probes, not part of CI
├── docs/
│   ├── architecture-zh.md       design rationale, in Chinese
│   ├── api-reference.md         the public surface, contract by contract
│   ├── testing.md               how a suite is written
│   ├── review/                  review records and their index
│   └── smart-docs/              this generated overview set
├── CMakeLists.txt               INTERFACE target mirroring the suite
├── CPPLINT.cfg                  80 columns, copyright filter
└── .github/workflows/c-cpp.yml  three configurations on every push
```

## Getting started

```cpp
#include "tcp-server-mt.h"      // -I<repo>/include  -std=c++17  -pthread
```

A complete program — configure, install an echo handler, start, stop — is in
[`api-reference.md`](../api-reference.md#quick-start). The two facts worth
knowing before the first run:

1. `config::port = 0` means "kernel, pick one" — read `server.port()` after
   `start()` if the application needs to publish it.
2. The library installs no signal handler. Shutdown is driven from the
   application's control thread.

Building and testing on this machine:

```bash
bash verify/run.sh                               # plain build
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh   # all three configurations
cmake -S . -B build && cmake --build build -j && ctest --test-dir build
```

## Architecture summary

Two kinds of thread share the work. An **accept thread** owns the listen socket
and hands each new descriptor to a **reactor**, chosen round-robin; it never
touches a connection after that. Each **reactor** runs one `epoll` loop over its
own connections, parses frames, invokes the application's handler on its own
thread, and owns both ring buffers of every connection it serves.

Cross-thread work — a reply produced by a worker, a close request from the
application, an offloaded task's result — travels as a task through the target
reactor's queue, with a single `eventfd` write to wake it. The design keeps all
per-connection state single-threaded and pushes the cost of sharing to a fixed
number of sharded containers.

[`02-architecture-overview.md`](02-architecture-overview.md) describes the
layers, the patterns and the decisions behind them;
[`03-workflow-overview.md`](03-workflow-overview.md) follows six concrete
flows through the code.
