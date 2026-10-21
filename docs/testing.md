# Testing

Eleven suites, thirteen runs, three configurations. `verify/run.sh` is the
authority; `CMakeLists.txt` mirrors it for `ctest`. What follows is how a suite
is written, and why the client half of every one of them now lives in a single
file.

## What runs

```
bash verify/run.sh                               # plain only
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run.sh   # 33 configurations
```

Each run is a separate build: ASan and TSan cannot be linked into one binary, and
a suite that only passes under a sanitizer is a different claim from one that
passes without. A hang is bounded by `timeout 180`; a failure is a non-zero exit,
which is the only thing the runner reads.

The suites live in `verify/`. The twenty programs under `verify/evidence/` are
not part of this: each settles one claim against a history that has since
changed, and they are not wired into the runner.

## Writing a suite

`verify/harness.h` is one header with four things in it:

| | |
|---|---|
| `harness::Fixture` | a started server: ephemeral port, handlers, stop on the way out |
| `harness::Peer` | one connection: `send`, `sendRaw`, `expectFrame`, `expectClose`, `readToEnd` |
| `harness::expect` / `finish` | the one assertion printer, and the exit code |
| `harness::freePort` | a port, for the cases that need the number before the server exists |

A suite that only speaks frames is about this long:

```cpp
#include "harness.h"

int main() {
    harness::Fixture server(harness::quietConfig(),
                            [](uint64_t, const uint8_t*, size_t) {});
    harness::expect(server.started(), "start() ok");

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(server.port())),
                    "client connected");
    peer.send(industrial::FrameType::PING, nullptr, 0);

    harness::Frame f;
    const harness::Status st = peer.expectFrame(&f, 500);
    harness::expect(static_cast<bool>(st), "a frame came back", st.why);
    harness::expect(f.type == industrial::FrameType::PONG, "and it is a PONG");

    return harness::finish();
}
```

`Fixture` takes up to three handlers -- message, close, conn-state -- because
between them they are what the suites need to observe. `stop()` is idempotent and
callable early, for the assertions that look at a stopped server.

## Two rules

**Printing is shared; judgement is not.** `harness::expect` prints and counts;
which invariant to assert stays in the suite, where it can be read. A harness that
decided what to assert would hide the claim at the exact moment someone needs to
check it.

**Raw sockets are allowed when the wire is the subject.** `sendRaw` exists for
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

The one line that legitimately moves is `acceptErrors` in the fork case: the
untouched binary varies between 1016, 1009 and 1010 across runs, so a match there
would be luck rather than evidence.

## What it caught

`harness_selftest.cpp` tests the harness against a bare socket, because a wrong
harness makes every suite pass for the wrong reason. It found one immediately:
after a reset has been reported once, `recv()` returns 0, which is
indistinguishable from an orderly FIN, so a second `expectClose(Fin)` relabelled a
reset connection as a clean one. `Peer` now remembers what it observed.

Running the full 33 configurations -- not only the suites that were touched --
caught the other: `portIsListening` closed its probe descriptor twice when the
connect succeeded, and the second close could land on a descriptor number the
accept loop had just handed out. TSan reported it once, in a suite the change had
not touched, and the binary was green on five standalone runs. It was real, and
rare, and only a full run made it likely enough to see.

## The self-test

`harness_selftest.cpp` drives `Peer` against a listening socket it controls, and
asserts six things: framing on send, reassembly when a frame arrives split, FIN
recognised, RST recognised and not relabelled, timeout reported as a timeout, and
`sendRaw` writing the bytes it was given. It starts no server, so a failure there
is always the harness.
