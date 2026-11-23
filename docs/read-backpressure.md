# Read-side backpressure — watermark design

Status: **implemented** · design 2026-11-13 · landed in `33f0479`, together
with the `EPOLLHUP|EPOLLERR` ordering fix it depended on.

Implementation note: the shipped configuration names are
`read_high_watermark_bytes` / `read_low_watermark_bytes` (0 means 3/4 and 1/2
of the effective `max_pending_bytes`), and the counters are `read_pauses` plus
the already-existing traffic counters — not the proposed `read_resumes` /
`pause_timeouts`. The proposed dedicated `peer not draining` pause deadline was
not added: a paused connection is covered by `idle_timeout_ms`, with drain
progress counted as activity, and the frame-assembly sweep is exempted while
paused. The interest mask is applied through `apply_interest()` on every
transition; the cached-mask optimisation described in *State and interest
model* below was not adopted.

## Context

The reactor reads and parses as fast as the peer can send, with no regard for
whether the peer is draining replies. The only bound on owed output is
`pending_bytes` (`connection` 677), and it is enforced at two different places
with two different outcomes:

| Path | On exhaustion | Result |
|---|---|---|
| `submit_send` (DATA from the application) | `send_to` returns `overloaded` | the caller decides; connection survives |
| PONG in `parse_frames` (2255–2270) | `close_conn(..., false)` | **RST**, reason `pending bytes at limit on pong` |

So "the peer is reading slowly" is a soft condition for application data and a
fatal one for keepalive traffic. A client that pipelines PINGs and drains slowly
is reset rather than slowed, which is the wrong direction: the reactor could
simply stop reading and let the TCP window do the work.

`pending_bytes` is already the right signal. It covers both halves of what is
owed — frames still in a queued `send_task` and frames already in `wbuf` — and
every release runs on the owning reactor (comment at 651–677), so a watermark
built on it needs no new synchronisation.

## Goals

- Convert "peer not draining" from a reset into flow control: stop reading,
  let the receive window close, resume when the peer catches up.
- Make the DATA/PING asymmetry above disappear for connections that are merely
  slow, without changing what happens when the peer is actually gone.
- Keep the close path's guarantees intact: a graceful close must still be able
  to serve buffered input and reach FIN.
- Default behaviour unchanged when the feature is not configured.

## Non-goals

- No change to wire format, framing, or the close protocol's transitions.
- No global memory budget, no per-IP admission, no buffer growth policy (those
  are separate items of the hardening list).
- No change to how `overloaded` is reported to the application. The watermark
  reduces how often it is reached; it does not replace the return value.

## Model

Two thresholds on `pending_bytes` with hysteresis:

```
pending >= high          -> pause  (drop EPOLLIN, stop reading)
pending <= low           -> resume (re-arm EPOLLIN, read what is buffered)
```

`high` must leave room for at least the replies already in flight, and `low`
must sit far enough below `high` that one drain does not immediately re-cross
it. Concretely, the useful range is a few maximum frames; with the default
`max_pending_bytes = MAX_FRAME_SIZE` (65544) there is no room for hysteresis at
all, which is why the feature is opt-in and `start()` warns when it is enabled
without headroom (see *Configuration*).

The pause happens **at a frame boundary**: the parser stops before consuming
the next frame, so the frame stays in `rbuf` and is parsed by the resume. This
keeps `pending_bytes` honest (no half-served frame) and keeps the close path's
`close_input_bytes` accounting meaningful.

## State and interest model

The current implementation assumes EPOLLIN is always registered. `write_armed`
(625) describes the EPOLLOUT bit only, and `finish_peer_closed` (2114) already
breaks the assumption by issuing a MOD with `EPOLLOUT` alone for a parked
half-close. Pausing reads adds a second reason for EPOLLIN to be absent, so the
single bool must become an explicit mask:

```cpp
bool     read_armed  = true;     // EPOLLIN wanted
bool     write_armed = false;    // EPOLLOUT wanted
uint32_t interest    = EPOLLIN;  // last mask this reactor asked for
bool     read_paused = false;    // watermark state (not the same as !read_armed)
```

One helper replaces the two ad-hoc `epoll_ctl` call sites:

```cpp
// Returns false only when the connection was closed as a result.
bool update_interest(connection* c, const char* failure_reason);
```

It computes `(read_armed ? EPOLLIN : 0) | (write_armed ? EPOLLOUT : 0)`, skips
the syscall when that equals `interest`, otherwise issues `EPOLL_CTL_MOD` and
falls back to `EPOLL_CTL_ADD` on `ENOENT` (the recovery both `arm_write` and
`finish_peer_closed` already carry). On other failures it counts
`epoll_ctl_errors` and closes with `failure_reason`.

`interest` is *what the reactor last asked for*, not what the kernel holds — the
same distinction `write_armed` documents today (618–625). It is only used to
skip redundant MODs; correctness never depends on it being fresh.

**Pause state is not the same as `!read_armed`.** `finish_peer_closed` parks a
half-close with EPOLLIN deliberately off so a FIN'd level-triggered socket does
not refire. `read_paused` only describes the watermark. Keeping them separate
prevents the resume path from re-arming reads on a parked peer-close.

## Pause

Trigger, at the top of each `parse_frames` (2187) iteration and once in
`handle_read` (1897) before `recv`:

```cpp
if (!c->fsm.closing.load(std::memory_order_acquire) &&
    c->pending_bytes.load(std::memory_order_relaxed) >= high) {
    pause_read(c);
    return;                 // leave the frame in rbuf
}
```

`pause_read(c)`:

1. no-op if `read_paused`, `is_closed`, or `fd < 0`;
2. `read_paused = true`, `read_armed = false`, stamp `paused_at_ms`;
3. `unschedule_read(c)` — a queued continuation would otherwise run before the
   interest change takes effect;
4. `update_interest(c, "epoll read pause failed")`;
5. `++read_pauses` (new counter).

`handle_read` must not call `schedule_read` after a pause, and must not `recv`.
Both exits (`frames_left <= 0`, `rbuf.writable() == 0`) are reached only after
the parser returns; the pause check above is placed before them.

## Resume

`pending_bytes` decreases on the owning reactor at exactly four sites: the
drain (1887), the two PONG release paths (2255, 2265), `discard_pending_output`
(2405), and the closing-side send refusal (3614). Three of those are
close-path or teardown, where pause is already off. The only site that can
matter for a live connection is the drain, so the resume check goes there:

```cpp
const drain_outcome r = drain_wbuf_to_socket(c);
if (r != drain_outcome::error && r != drain_outcome::dead)
    maybe_resume(c);        // pending <= low && read_paused -> resume_read()
```

`maybe_resume` is a cheap guard (one relaxed load) and is called even when the
drain stalled on the byte budget — `pending_bytes` may already be below `low`
while `wbuf` still holds data.

`resume_read(c)`:

1. `read_paused = false`, `read_armed = true`, clear `paused_at_ms`;
2. if a frame is in progress, add the paused interval to
   `frame_started_at_ms` — the assembly clock measures *the peer's* dribbling,
   and the pause was ours; adding rather than re-stamping keeps the time spent
   before the pause on the peer's side of the ledger;
3. `update_interest(c, "epoll read resume failed")`;
4. `schedule_read(c)` — bytes may already be sitting in `rbuf`.

## Interaction with the close FSM

| Event while paused | Handling | Why |
|---|---|---|
| `close_connection` / `stop_gracefully` | `drain_input_then_close` (2521) calls `resume_read` first, then `handle_read` | The close owes the peer answers to input it already delivered. The close path already bounds this work: `close_input_bytes`, the one-round deferral, and `close_linger_ms`. |
| `closing` becomes true | watermark is ignored for the rest of the close | Submissions are refused from that point, so `pending_bytes` can only fall. Pausing would add a state to a path whose whole point is that it always makes progress. |
| idle sweep (`do_tick` 2703) | skipped while paused; a separate pause deadline applies | `last_recv_ms` stops moving because *we* stopped reading — closing for idleness would punish the peer for our own backpressure. |
| frame assembly sweep | skipped while paused; on resume the paused interval is added to the clock (step 2 above) | Same reason; the frame keeps the budget it had before the pause, so alternating pause and resume cannot buy a fresh one. |
| peer FIN | not observed until resume | EPOLLIN is off, and registering EPOLLRDHUP instead would refire level-triggered on every `epoll_wait`. The pause deadline is what bounds this. |
| `EPOLLHUP` after the HUP fix | if paused, HUP closes the connection (abort) | We cannot drain into a closed peer; the abort is the honest outcome. |
| `stop()` | unaffected | Teardown closes the fd; no interest call survives it. |
| write side | unaffected | `EPOLLOUT` continues to be driven by `write_armed`. |

The close's `rbuf` guarantee is a bound, not a count. `drain_input_then_close`
serves the frames already in `rbuf` when the close runs; frames still in the
kernel's receive buffer are not part of the promise, and how many PONGs are owed
from the backlog varies with its size, the drain rate and `close_input_bytes`.
A test that asserts "every request sent before the close was answered" is
asserting more than the design offers; case B2 in `verify/watermark.cpp` can
assert the whole backlog only because its pending ceiling lets the drain round
serve it.

Measured rather than argued
(`verify/evidence/probe_close_while_paused.cpp`): 4000 requests queued in the
kernel of a paused connection, one close — 256 replies, then FIN. The other 3744
were parsed and handed to the handler (`frames_recv` 4000) and never reached the
wire. The public contract now says this in
`close_connection` (docs/api-reference.md) and at `drain_input_then_close`; it
used to be a property of the implementation that only this document knew.

The pause deadline is a new deadline, not a reuse of `idle_timeout_ms`: a paused
connection may legitimately stay paused for as long as the peer takes to read,
and aborting it with reason `idle timeout` would misreport why. Proposed
resolution: `read_pause_timeout_ms = 0` means `2 * idle_timeout_ms`, checked in
`do_tick` before the idle sweep, closing with a new reason
**`peer not draining`** and `graceful = false` (there is nothing to drain into).

## Configuration

```cpp
size_t pending_high_watermark = 0;   // 0: feature off
size_t pending_low_watermark  = 0;   // 0 with high set: max_pending_bytes / 4
int    read_pause_timeout_ms  = 0;   // 0: 2 * idle_timeout_ms
```

Validation in `start()`, after `max_pending_bytes` is clamped to
`wbuf_capacity` (3006–3008):

- both watermarks zero → feature off, no behaviour change, no extra syscalls;
- `high <= low` → refuse to start (a hysteresis that does not exist would flap);
- `high > max_pending_bytes` → clamp with a warning;
- `high - low < MAX_FRAME_SIZE` → warn: one maximum frame can cross the whole
  band, which turns the watermark into a per-frame MOD pair.

The last point is the practical one: enabling this feature on the default
configuration is not useful. The documented recommendation is
`max_pending_bytes >= 4 * MAX_FRAME_SIZE` with `high = 3/4` and `low = 1/4` of
it, which gives a two-frame band.

## Counters

| Counter | Moves when |
|---|---|
| `read_pauses` | `pause_read` changes state |
| `read_resumes` | `resume_read` changes state |
| `pause_timeouts` | `do_tick` aborts a paused connection |

All three are reactor-thread-only and can be published with the existing batched
tally, like the traffic counters.

## Invariants

1. `read_paused` is set only when `!closing && !is_closed && fd >= 0`.
2. Every `pause_read` is matched by exactly one `resume_read`, a teardown, or a
   pause timeout.
3. `resume_read` always precedes a close-path read; no close task can observe
   `read_paused`.
4. The watermark never delays teardown: `teardown_closed` is reachable from
   every paused state.
5. With the feature off, the only new cost is one relaxed load per `handle_read`
   and one per `handle_write`.

## Test plan

1. **Slow reader, pipelined PINGs.** Client sends 10 000 PINGs without reading,
   then drains. Assert: no `pending bytes at limit on pong`, `read_pauses > 0`,
   all 10 000 PONGs delivered, `aborted == 0`.
2. **Hysteresis.** Same workload with the band at two frames; assert
   `read_pauses` does not exceed `pending / low` by more than a small constant
   (no per-frame flapping).
3. **Paused then closed.** Pause, then `close_connection` from the controlling
   thread. Assert graceful close, `aborted == 0`, every queued reply delivered.
4. **Paused and abandoned.** Client stops reading and stops sending. Assert the
   close reason is `peer not draining` at `read_pause_timeout_ms`, not
   `idle timeout`.
5. **Paused and half-closed.** Client pauses reading, then sends FIN. Assert the
   connection survives until resume or the pause deadline, never a busy loop
   (`eof_reads` flat, CPU idle).
6. **Paused through `stop_gracefully`.** Assert the whole set drains within
   `close_linger_ms`.
7. **Regression with the feature off.** All nine suites and the evidence probes
   unchanged.
8. **TSan.** Pause/resume touches only reactor-owned fields; the suite must stay
   clean under the existing `tsan` configuration (`verify/run.sh:163`).

## Phasing

| Phase | Content | Risk |
|---|---|---|
| 0 | `interest`/`read_armed`/`write_armed` refactor, no behaviour change | mechanical, covered by existing suites |
| 1 | `pause_read` / `resume_read`, config, counters, do_tick interactions | the feature itself |
| 2 | PING reserve failure pauses instead of closing | needs `parse_frames` to stop consuming the header before the PONG is queued (today it consumes at 2218, before the type dispatch) |
| 3 | Docs: `api-reference.md` (config, counters, close reasons), `README.md`, `testing.md` | — |

Phase 2 is deliberately separate: it changes how a *received* frame is retained,
which is the one place where "the parser always consumes what it looks at" stops
being true.

## Open questions

1. **Default on or off?** Recommended: off (0/0), because a single-header
   library must not change the behaviour of existing deployments on upgrade.
2. **Pause deadline basis:** `2 * idle_timeout_ms`, or tied to
   `close_linger_ms`? The former scales with the operator's notion of patience;
   the latter with their notion of a bounded fd cost.
3. **Is phase 2 in scope now?** Without it, a PING that arrives just before the
   band is crossed can still produce the reset this design exists to remove.
