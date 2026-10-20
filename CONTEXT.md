# CONTEXT.md

The load-bearing words of this repository. They live only in comment prose and in the
eight review documents, so a new reader (or an agent) trying to understand the two values
of `deferredRound` has to read the comment above `CloseRound` and then find
`docs/review/CODE-REVIEW-2026-10-18-round7.md` to learn why (line 10 of that document is
"one bool shared by two owners, meaning opposite things").

Each entry gets a definition, the mistake it exists to prevent, and where it lives.
**The counterexamples are the useful half** — these words need defining precisely because
each one has already been misread once, and each misreading became a real defect.

---

## What this project is

A Linux TCP server in one header: C++17, level-triggered epoll, one accept thread plus N
sub-reactors. No `.cpp` files, no third-party dependencies. The product promise is "copy
`include/tcp-server-mt.h` into your project".

Which means: **readability and maintainability are the product here**, not a side metric.

---

## The words

### phase

Four stages **derived** from three flags — `isClosed` / `closing` / `finSent`:
`Open` → `Closing` → `FinSent` → `Torn`. `Conn::phase()` is the only place it is computed;
`decide()` is the only place it is consulted.

> **The mistake it prevents**: storing phase as a field. That makes it a fourth flag that
> has to be kept in step with the other three — and "fields that must be kept in step" is
> exactly what this mechanism exists to remove.

See: `ClosePhase`, `decide()`, `docs/architecture/r3-baseline.md` section 3.

### park

A middle state of a graceful close: the connection is still there, the fd is still there,
reads are still running, but **new submissions are refused** while wbuf drains (`Park`) or
while waiting for the peer's FIN.

> **The mistake it prevents**: reading park as closed. A parked fd is still registered in
> epoll and still findable by `sendTo`; it is `submitSend` that refuses new frames, and it
> counts them in `queueDrops` + `bytesDropped`.

See: `parkClose()`, `CloseAction::Park`.

### defer a round

"Wait one more queue batch" during a close. Two values, **and they must stay separate**:

- `Close` — owed by `drainInputThenClose()`; the round ends in a park.
- `PeerClosed` — owed by `finishPeerClosed()`; the round ends when that function decides
  again.

> **The mistake it prevents**: one bool meaning "the round has been spent". That was the
> code before round 8, and it cost the peer its last reply — read as "round spent, park
> now" when the truth was "the round is still owed", it parked one round early, wbuf was
> still empty, the FIN went out immediately, and it ran over replies whose own deferral had
> not executed yet.

See: `CloseRound`, `Conn::close.round`.

### linger

How long we are willing to wait for the peer's FIN after sending our own
(`closeLingerMs`). Past that, abort.

> **The mistake it prevents**: reading linger as "waiting for the peer to answer". The peer
> had already finished talking when it sent its FIN; what this window waits for is the
> peer's own FIN, not content.

See: the linger branch in `doTick()`, `Config::closeLingerMs`.

### graceful / abort

Two exits, and the difference is visible on the wire:

- `graceful` — deliver everything owed to the peer, then `shutdown(SHUT_WR)`, then
  `close()` once the peer's FIN arrives.
- `abort` — discard what is queued and ask the kernel for a RST via `SO_LINGER{on,0}`.

> **The mistake it prevents**: assuming `close(fd)` is a graceful close. When the peer still
> has unread data, `close()` sends a RST and discards queued output — which is the entire
> reason this machinery exists.

See: `unregisterConn()`, and P0-5 with cases C/E/F in `docs/review/`.

### admission

One RAII ticket per connection; `admitted_` is the actual gate on the connection limit.
Every failure and discard path returns it through the same destructor.

> **The mistake it prevents**: using `conns_.size()` as the connection count. That is the
> index size, not the admission count — the two disagree briefly on registration failure
> and on rejected connections, and `admitted_` is what the accept loop reads.

See: `Admission`, `acceptBatch()`.

### credit / pendingBytes

Bytes promised to the peer but not yet handed to the kernel: both frames still sitting in
queued send tasks and frames already in wbuf. One number covers both because neither alone
says whether the peer is being kept up with.

> **The mistake it prevents**: treating `wbuf.empty()` as "nothing owed". While wbuf is
> empty there can still be reserved frames in the queue — the long comment in `handleWrite`
> exists to explain why a `pendingBytes` guard cannot be added there.

See: `Conn::out.pendingBytes`.

### drain

Pushing wbuf into the kernel. Four outcomes (`DrainOutcome`), **not interchangeable**:

- `Drained` — wbuf is empty; carry on.
- `Stalled` — budget spent or EAGAIN; EPOLLOUT will pick it up.
- `Dead` — EPIPE; this direction will never send again.
- `Error` — anything else, fatal.

> **The mistake it prevents**: the old `0 / 1 / -1` encoding, where `Drained` and `EPIPE`
> shared the `0`. Treating them as one thing hands the connection's fate to
> `wbuf.empty()` at the call site — a fact no call site ever stated out loud.

See: `DrainOutcome`, `drainWbufToSocket()`.

### peerFin

`recv()` returned 0. The peer closed its write direction, but it may still be reading.

> **The mistake it prevents**: closing on FIN. That turns the reply we owe the peer into a
> RST. The correct order is: deliver what is owed, send our FIN, then wait for the peer to
> close.

See: `Conn::close.peerFinSeen`, `finishPeerClosed()`.

---

## Where to start reading

| If you want to change | Read first |
|---|---|
| Close behaviour | `docs/architecture/r3-baseline.md` section 5 (five decision tables) |
| Why the close works this way | the eight-round index in `docs/review/README.md`, following P0/P1 to the scene |
| Performance numbers | `verify/bench.sh` plus the before/after in the relevant commit message |
| The public API | `verify/evidence/probe_surface_shrank.sh` (it defines what must not be visible) |
| The ring buffer contract | `verify/ring_buffer.cpp` (more accurate than the comments) |

---

## One rule

This repository does not accept "looks more correct". Every change needs either a test that
can fail or a number that can be compared. The last line of `docs/review/README.md` is the
most important convention here:

> Nothing was written down as a defect without runtime evidence.

The converse holds too: **a "fix" without runtime evidence does not count either.**
