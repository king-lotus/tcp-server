# Testing

Fifteen suites, seventeen runs, three configurations. `verify/run.sh` is the
authority; `CMakeLists.txt` mirrors it for `ctest`. What follows is how a suite
is written, and why the client half of every one of them now lives in a single
file.

## What runs

```
bash verify/run.sh                               # plain only
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh   # 51 configurations
```

Each run is a separate build: ASan and TSan cannot be linked into one binary, and
a suite that only passes under a sanitizer is a different claim from one that
passes without. A hang is bounded by `timeout 180`; a failure is a non-zero exit,
which is the only thing the runner reads.

The suites live in `verify/`. Most probes under `verify/evidence/` are not part
of this: each settles one claim against a history that has since changed. The
EMFILE probe is the exception — `verify/evidence/run_emfile.sh` builds and runs
it with assertions, CI calls that script in its own step, and
`ENABLE_SLOW=1 bash verify/run.sh` adds it to a local run. It is deliberately
outside the three-configuration matrix: the claim is about accept-queue
behaviour under a low descriptor ceiling, not about sanitizers. A host that
never reaches EMFILE prints `[SKIP]` instead of a pass.

### The fuzz entry point

`verify/frame_codec.cpp` defines `LLVMFuzzerTestOneInput` alongside the suite's
own deterministic driver. Both call the same invariant checker, so the ordinary
matrix exercises the fuzz entry point even on machines without clang's fuzzer.
A real session is one command:

```bash
clang++ -std=c++17 -Iinclude -DTCP_FUZZING=1 -fsanitize=fuzzer,address verify/frame_codec.cpp -o decode_header_fuzz
./decode_header_fuzz -dict=verify/fuzz/decode_header.dict -max_len=12 -max_total_time=60
```

The dictionary is not decoration: two specific magic bytes out of 65536 are a
rare mutation to find from empty input, and a fuzzer without them spends its
time in `need_more`. `TCP_FUZZING=1` drops the suite's `main()` and its
`harness.h` dependency, leaving the pure header and the checker.

### The syscall seam

Six calls — `recv`, `sendmsg`, `shutdown`, `epoll_ctl`, `accept4` and the
reactor's eventfd wake write — go through `syscall_policy::` rather than `::`
directly. Five exist to inject failures; the wake write exists so a test can
count notifications without adding a production counter to every reactor. The
production policy is static inline forwarders, so this is one
direct call at `-O0` and less at `-O2`: no virtual, no pointer, no branch, no
per-server state.

A test binary may replace the whole policy by defining
`TCP_SERVER_SYSCALL_POLICY` before any include of `tcp-server-mt.h`.
`verify/syscall_seam.cpp` does that with `verify/syscall_seam_policy.h`, whose
knobs are one-shot errnos consumed with `exchange(0)` on the reactor thread.
That turns five failure paths into ordinary assertions, with no fork, no
lowered `RLIMIT_NOFILE` and no attempt to manufacture a real EPIPE window:

- `recv` error → abort, not a spurious FIN;
- `sendmsg` EPIPE → counter moves, no EPOLLOUT spin, peer sees RST;
- short `sendmsg` → the remaining bytes reach the peer and the frame count is
  still one; a five-byte write across three iovecs verifies the injection
  helper walks every caller segment rather than indexing a fixed array;
- `shutdown` failure → a parked close ends in RST rather than an unearned FIN;
- `epoll_ctl(MOD)` failure → `epoll_ctl_errors` moves and the connection aborts;
- `accept4` EMFILE → the reserved descriptor takes the queued victim, EMFILE is
  counted, and the accept loop still serves the next client;
- `accept4` EINTR → retried inside the same bounded round, and the admitted
  connection is then served;
- a non-EINTR `accept4` error → ends only that round; the thread survives and
  the next readable listen event admits the queued connection. These two cases
  pin the distinction that a bare `break` used to hide.
- 512 queued sends while the reactor is blocked → at most one eventfd wake,
  with all frames drained afterward. This tests coalescing rather than timing.

The macro is a per-binary choice, not a per-file one. Every translation unit
linked into a binary must see the same `syscall_policy` alias; `run.sh` and
CMake compile this suite as its own target for that reason. Each case asserts
both that its policy knob was consumed and that the observable result changed,
so the suite tests the injection path rather than the loopback socket.

### Latency histograms

`config::latency_histograms` is opt-in: three histograms — `loop_lag_us`,
`handler_duration_us` and `queue_wait_us` — stay empty by default, so a server
that never enables the flag pays nothing for them. Bucket edges are fixed
powers of two in microseconds, with a final `>=1024us` bucket for the long
tail. `verify/latency_histograms.cpp` asserts the flag-off case (all three
empty), a sleeping handler's recorded duration, and queue-wait growth while
the reactor is blocked.

## Writing a suite

`verify/harness.h` is one header with four things in it:

| | |
|---|---|
| `harness::fixture` | a started server: ephemeral port, handlers, stop on the way out |
| `harness::peer` | one connection: `send`, `send_raw`, `expect_frame`, `expect_close`, `read_to_end` |
| `harness::expect` / `finish` | the one assertion printer, and the exit code |
| `harness::free_port` | a port, for the cases that need the number before the server exists |

A suite that only speaks frames is about this long:

```cpp
#include "harness.h"

int main() {
    harness::fixture server(harness::quiet_config(),
                            [](uint64_t, const uint8_t*, size_t) {});
    harness::expect(server.started(), "start() ok");

    harness::peer peer;
    harness::expect(static_cast<bool>(peer.connect(server.port())),
                    "client connected");
    peer.send(industrial::frame_type::PING, nullptr, 0);

    harness::frame f;
    const harness::status st = peer.expect_frame(&f, 500);
    harness::expect(static_cast<bool>(st), "a frame came back", st.why);
    harness::expect(f.type == industrial::frame_type::PONG, "and it is a PONG");

    return harness::finish();
}
```

`fixture` takes up to three handlers -- message, close, conn-state -- because
between them they are what the suites need to observe. `stop()` is idempotent and
callable early, for the assertions that look at a stopped server.

## Two rules

**Printing is shared; judgement is not.** `harness::expect` prints and counts;
which invariant to assert stays in the suite, where it can be read. A harness that
decided what to assert would hide the claim at the exact moment someone needs to
check it.

**Raw sockets are allowed when the wire is the subject.** `send_raw` exists for
frames that must be wrong on purpose -- a bad magic number, a length past
`MAX_FRAME_PAYLOAD`, half a header. Three places keep their own sockets entirely,
and none of them is plumbing:

- `bench_*` measure the bare socket path; a peer in the middle would measure the
  peer.
- `send_results` forks a child that lowers its own descriptor ceiling, and asks
  the kernel whether it reports a broken socket at all before asserting on it.
- `close_semantics` reads to the end of a connection and counts the bytes that
  arrived, which is the subject of the file rather than a way to reach it.

## What the harness is not

It is not a test framework: no setup/teardown hooks, no mocking, no assertion
library beyond printing a line and counting it. It does not know anything about
the server's internals -- everything goes through the wire. And it is not used by
the benchmarks, on purpose.

## How the migration was verified

One suite at a time, and each time the same way: build the binary from the commit
before the migration, save its output, migrate, run again, and require `diff` to
be empty. Normalising the ephemeral port is allowed; nothing else is.

That caught one regression the compiler could not. `send_results` printed
`peers.size()` after the peers had been cleared, so the count went from 40 to 0
while every assertion still passed. The output diff is what noticed.

The one line that legitimately moves is `accept_errors` in the fork case: the
untouched binary varies between 1016, 1009 and 1010 across runs, so a match there
would be luck rather than evidence.

## What it caught

`harness_selftest.cpp` tests the harness against a bare socket, because a wrong
harness makes every suite pass for the wrong reason. It found one immediately:
after a reset has been reported once, `recv()` returns 0, which is
indistinguishable from an orderly FIN, so a second `expect_close(fin)` relabelled a
reset connection as a clean one. `peer` now remembers what it observed.

running the full 51 configurations -- not only the suites that were touched --
caught the other: `port_is_listening` closed its probe descriptor twice when the
connect succeeded, and the second close could land on a descriptor number the
accept loop had just handed out. TSan reported it once, in a suite the change had
not touched, and the binary was green on five standalone runs. It was real, and
rare, and only a full run made it likely enough to see.

## The self-test

`harness_selftest.cpp` drives `peer` against a listening socket it controls, and
asserts six things: framing on send, reassembly when a frame arrives split, FIN
recognised, RST recognised and not relabelled, timeout reported as a timeout, and
`send_raw` writing the bytes it was given. It starts no server, so a failure there
is always the harness.
