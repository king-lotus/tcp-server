# API reference

`include/tcp-server-mt.h` is the whole library: one header, no translation unit
to link, no third-party dependency. This document covers the surface it exposes
— the config struct, the server class, its three callbacks, `send_result`, and
`server_stats` — and the behaviour a caller is entitled to rely on. It applies
from revision `33f0479` onward; the header is authoritative if the two disagree.

For why the internals are shaped this way, see
[`architecture-zh.md`](architecture-zh.md). For how the behaviour is asserted,
see [`testing.md`](testing.md).

## Requirements

Linux (the implementation uses `epoll` and `eventfd`), C++17, and `-pthread`.
Compile with `-I<repo>/include`.

## Threading model

- **One accept thread plus N sub-reactors.** Each reactor runs on its own
  thread and owns the connections assigned to it.
- **A message handler runs on the reactor that owns the connection.** Frames
  from one connection are therefore handled serially and in order; different
  connections, on different reactors, are handled concurrently.
- **`send_to`, `close_connection` and `offload` may be called from any thread**,
  including from inside a handler. A send issued on the owning reactor writes
  straight into the write buffer; from anywhere else it is queued to the owner
  with a wakeup.
- **`start`, `stop`, `stop_gracefully` and `wait` belong to one controlling
  thread**, and that thread must not be a server callback. Callbacks run on
  threads the server owns; a callback cannot join them.
- **The library installs no signal handler.** Shutdown is started by the
  application: its `SIGINT`/`SIGTERM` handling has to hand off to the control
  thread (a flag or a self-pipe), which then calls `stop_gracefully()` or
  `request_stop()`. A lifecycle call is not safe from a signal handler itself.

## Wire protocol

Every frame is an 8-byte big-endian header followed by up to 64 KiB of payload.

| Offset | Size | Field | Value |
|---:|---:|---|---|
| 0 | 2 | magic | `0xEB90` |
| 2 | 2 | type | `1` DATA, `2` PING, `3` PONG |
| 4 | 4 | length | payload bytes, ≤ 65536 |
| 8 | length | payload | opaque to the library |

- The largest legal frame is 65544 bytes; `send_to` refuses a larger payload
  before it touches the socket.
- A **PING** is answered by the library with an empty PONG and is not delivered
  to the handler.
- A **PONG** is consumed silently.
- **The type space is closed: `1`, `2`, `3`, and nothing else.** Any other value
  says the peer speaks a different protocol, not that its stream is corrupt, so
  the frame is not delivered: the connection is closed with
  `unknown frame type`, with a reset rather than a FIN, and
  `unknown_frame_types` is incremented. The decision needs only the header, so
  it is made as soon as those eight bytes arrive, not after the declared
  payload does; a connection paused by read backpressure parses nothing, this
  header included, until it resumes. The payload is left unread — once the
  type is unknown, the rest of the stream is unknowable, and there is nothing
  to resynchronise to.
- An application that needs its own opcodes should carry them in the DATA
  payload (one byte gives 256, two give 65536). That keeps the type space
  something the library can validate on the peer's behalf.
- `config::allow_unknown_frame_types` restores the older behaviour, where every
  other type was delivered to the message handler as if it were DATA. It is
  **off by default**, and turning it on is a trust decision the application
  takes: nothing authenticates the peer in front of this entry point, so an
  unknown type reaching the handler is untrusted input selecting an application
  code path.
- A bad magic, an over-length frame, an unknown type, a ring buffer too small to
  hold the frame, or a received frame that cannot be answered (see [close
  reasons](#close-reasons)) ends the connection with a reset rather than a
  partial delivery.
- The inline pure function `decode_header(const uint8_t*, size_t, bool
  allow_unknown_types)` makes those header decisions without a socket, a
  connection or a counter. It returns a `decoded_header` whose `status` is one
  of `need_more`, `ok`, `bad_magic`, `unknown_type`, `frame_too_large`; `type`
  and `len` carry a value only when the status is `ok`. The check order —
  magic, then type, then length — is part of the protocol. `parse_frames`
  maps the status onto a close reason and a statistic; `verify/frame_codec.cpp`
  exercises the same function with a table, an equivalence check against the
  old byte order, and a libFuzzer entry point.

## Quick start

```cpp
#include "tcp-server-mt.h"

#include <cstdio>

int main() {
    industrial::multi_reactor_server::config cfg;
    cfg.port = 9000;                 // 0 lets the kernel pick one

    industrial::multi_reactor_server server(cfg);

    // Echo. Runs on the reactor that owns the connection.
    server.set_message_handler(
        [&server](uint64_t id, const uint8_t* data, size_t len) {
            (void)server.send_to(id, data, len);
        });

    server.set_close_handler(
        [](uint64_t id, const std::string& peer, const char* reason) {
            std::printf("conn %llu (%s) closed: %s\n",
                        static_cast<unsigned long long>(id), peer.c_str(),
                        reason);
        });

    if (!server.start()) return 1;
    std::printf("listening on %u\n", server.port());

    // The application's own control loop runs here. On shutdown:
    const bool drained = server.stop_gracefully(5000);
    return drained ? 0 : 2;
}
```

## `config`

Set fields on the struct, then pass it to the constructor. The configuration is
validated by `start()`; nothing is checked at construction.

| Field | Default | Effect |
|---|---|---|
| `bind_addr` | `"0.0.0.0"` | Listen address. Defaults to every interface. |
| `port` | `0` | Listen port. `0` asks the kernel to choose; read `server.port()` afterwards. |
| `backlog` | `1024` | `listen()` backlog. Must be > 0. The kernel may cap it (`somaxconn`). |
| `reuse_port` | `false` | Sets `SO_REUSEPORT`. Off by default: with it on, a second process can bind the same port and the kernel load-balances connections between them. |
| `num_reactors` | `0` | Reactor threads. `0` means `hardware_concurrency()`; capped at 64. |
| `max_connections` | `10000` | Admission ceiling. A connection over it is accepted and immediately closed, counted in `rejected`. The memory this implies is bounded separately by `max_total_buffer_bytes`. |
| `max_total_buffer_bytes` | `0` | Ceiling on buffer bytes admitted connections may reserve. `0` means `max_connections × (rbuf_capacity + wbuf_capacity)`. `start()` refuses a value below one connection's buffers. Rejections are counted in `buffer_budget_rejections`. |
| `max_connections_per_ip` | `0` | Most live connections one peer address may hold; `0` is unlimited. IPv6 is keyed by `/64`, because the low 64 bits are chosen by the peer. Rejections are counted in `ip_rejections`. |
| `idle_timeout_ms` | `120000` | Close a connection after this long with **no activity**, where activity is bytes received *or* bytes drained to the peer: a connection paused by backpressure is not receiving by design, and a slow reader is not idle. Must be > 0. |
| `tick_ms` | `500` | Granularity of every deadline. Must be > 0; a tick coarser than a deadline makes that deadline late by up to one tick, and `start()` warns. |
| `reactor_stall_timeout_ms` | `0` | Watchdog for a reactor alive but not completing a loop iteration for this long. 0 disables it; otherwise it must be at least `max(100, 2 × tick_ms)`. One iteration is `tick_ms` in `epoll_wait` plus the loop body (event batch, pending tasks, `do_tick`), so a timeout just over `tick_ms` reports healthy reactors; it must also exceed the longest expected body. With the watchdog on, the accept thread samples at least every `min(500 ms, timeout/2)`, so a stall shorter than the timeout plus one sample can fall between samples. A stall has two effects, both on the accept side: the episode is counted once (`reactor_stalls`) and logged, and the reactor stops receiving **new** connections (`reactor_stall_skips`) until it has made progress for a full timeout window again. Its existing connections are not touched, and the server is never stopped. |
| `frame_assembly_timeout_ms` | `0` | Deadline for one frame, measured from its first byte. `0` means 5 × `idle_timeout_ms`. Must be ≥ 0. |
| `allow_unknown_frame_types` | `false` | Deliver a type outside `1`/`2`/`3` to the message handler as DATA instead of closing the connection. See [wire protocol](#wire-protocol). |
| `rbuf_capacity` | `65544` | Per-connection read buffer. Must be ≥ 65544: a smaller one cannot hold a legal maximum frame. |
| `wbuf_capacity` | `65544` | Per-connection write buffer. Must be ≥ 8. |
| `max_pending_bytes` | `65544` | Ceiling on bytes accepted for one connection but not yet written. Clamped down to `wbuf_capacity` by `start()`. |
| `socket_send_buffer_bytes` | `0` | Caps `SO_SNDBUF` on each accepted socket; `0` leaves the OS default. A smaller buffer makes backpressure engage sooner and bounds the kernel memory one connection can pin. |
| `read_high_watermark_bytes` | `0` | Above this pending-output level the reactor stops reading and drops `EPOLLIN`, which is what lets TCP push back on a peer that writes faster than it reads. `0` means 3/4 of the effective `max_pending_bytes`. |
| `read_low_watermark_bytes` | `0` | Reading resumes below this level. `0` means half the high mark. Explicit values must satisfy `0 < low < high ≤ max_pending_bytes`. |
| `write_budget_bytes` | `262144` | Most bytes written to one socket in a single drain before control returns to the reactor, so one fast sender cannot monopolise it. Must be > 0. |
| `close_linger_ms` | `5000` | Total budget for a graceful close, counted from the moment it is parked: drain the write buffer, send the FIN, then wait for the peer's FIN. Must be > 0. |
| `handler_threads` | `0` | Size of the optional offload pool. `0` disables `offload()` entirely; capped at 64. |
| `handler_queue_limit` | `8192` | Queued offload tasks across the whole server. Must be > 0 when `handler_threads > 0`. |

Helpers resolve the "0 means decide for me" fields for callers that need the
same numbers the server used: `resolved_reactor_count()`,
`resolved_frame_assembly_timeout_ms()`, `resolved_max_total_buffer_bytes()`,
`per_connection_buffer_bytes()`, and
`resolved_read_high_watermark_bytes()` / `resolved_read_low_watermark_bytes()`.
The last two are resolved after `start()` clamps `max_pending_bytes`, so they
are the values the reactors actually use.

## `multi_reactor_server`

### Construction and destruction

```cpp
explicit multi_reactor_server(config cfg) noexcept;
~multi_reactor_server() noexcept;
```

Non-copyable. The destructor calls `stop()`, so destroying a running server is
safe **from the controlling thread**. It is not safe from a callback: the
threads it would have to join are the ones running the callback, and
`std::thread::~thread` terminates on a joinable thread. The destructor detects
that case and prints a diagnostic before the terminate happens.

Configure handlers before `start()` and do not replace them while running.

### Callbacks

```cpp
using message_handler    = std::function<void(uint64_t conn_id,
                                              const uint8_t* data, size_t len)>;
using conn_state_handler = std::function<void(uint64_t conn_id,
                                              const std::string& peer,
                                              bool open)>;
using close_handler      = std::function<void(uint64_t conn_id,
                                              const std::string& peer,
                                              const char* reason)>;
```

- `message_handler` — one complete DATA frame (or any unknown type). `data`
  points into library-owned scratch space and is borrowed for the call only; a
  handler that needs it later must copy it. A throw from this handler is caught
  and dropped; it does not close the connection.
- `conn_state_handler` — called with `open == true` when a connection is
  admitted and with `false` when it is gone. `peer` is `"host:port"`, borrowed
  for the call.
- `close_handler` — called once per closed connection, with a short reason
  string (see [close reasons](#close-reasons)). `reason` is borrowed and valid
  only for the call; it must not be stored.

Both close-side callbacks (the `false` transition and `close_handler`) are
**deferred**: they run on the owning worker after the connection's stack frame
has unwound, which is what makes `stop()` from a callback safe. A throwing
close-side callback is contained: the exception is counted in
`callback_exceptions`, the other notification callback still runs, and the
server keeps serving.

The application is told about a close exactly once. If the notification cannot
be allocated, the connection still goes away and `close_callbacks_dropped`
records that the notification was lost.

### Handlers

```cpp
bool set_message_handler(message_handler h) noexcept;
bool set_conn_state_handler(conn_state_handler h) noexcept;
bool set_close_handler(close_handler h) noexcept;
```

Install before `start()`. Each returns `false` and leaves the handler unchanged
unless the lifecycle is fully `stopped`; the refusal is counted in
`handler_installs_refused`. The check is made under the same mutex `start()` and
`stop()` take, so a setter racing either one is refused rather than queued
behind it — which is also why a setter called from a callback cannot deadlock
`stop()`'s join. A caller that ignores the result keeps the old handler running,
and that is otherwise invisible: no callback fires, no error is reported, and
the code reads as though the install worked.

### Lifecycle

```cpp
bool start() noexcept;
bool stop_gracefully(int timeout_ms) noexcept;
void stop() noexcept;
void request_stop() noexcept;
void wait() noexcept;
```

**`start()`** binds, listens and starts the threads. It returns `false` — with
nothing running and no threads left behind — when the server is already
started, when called from a callback, or when the configuration is rejected:
`rbuf_capacity < 65544`, `wbuf_capacity < 8`, `max_pending_bytes < 8`,
`max_connections <= 0`, `tick_ms <= 0`, `idle_timeout_ms <= 0`,
`write_budget_bytes == 0`, `close_linger_ms <= 0`, `backlog <= 0`,
`frame_assembly_timeout_ms < 0`, `reactor_stall_timeout_ms < 0` or a positive
`reactor_stall_timeout_ms < max(100, 2 × tick_ms)`, `handler_threads < 0`,
`handler_threads > 64`, `handler_threads > 0` with
`handler_queue_limit == 0`, `max_connections_per_ip < 0`,
`max_total_buffer_bytes` below one connection's buffers, or read watermarks that
are not `0 < low < high ≤ max_pending_bytes`.

`start()` also inspects `RLIMIT_NOFILE`: it tries to raise the soft limit to the
hard limit, and warns when the limit is below
`max_connections + 2 × (1 + reactors) + 16` — the accept epoll/eventfd pair, one
epoll/eventfd pair per reactor, the spare descriptor and a small margin. When
the limit is tight, connections are still refused before `max_connections` is
reached; each refusal is counted in `accept_errors`.

**`stop()`** is immediate and abortive: stop accepting, close every connection,
join everything. Connections still holding queued output are torn down rather
than drained. Calling it twice is harmless. Called from a callback it only
requests shutdown and returns — the controlling thread must still call it.

**`stop_gracefully(timeout_ms)`** stops accepting, then asks every connection to
close gracefully, repeating for up to `timeout_ms` (polling every 10 ms) before
falling through to `stop()`. It returns `true` when no connections and no
admission reservations remain, `false` otherwise — including when it was called
from a callback, in which case it only requests shutdown. With `timeout_ms <= 0`
the drain window is empty and it falls straight through to `stop()`.

Note the exact condition behind that return value: the connection table and the
admission count are empty, which a connection aborted at the end of
`close_linger_ms` satisfies just as well as one that completed its FIN
exchange. A `true` result therefore means "nothing is left open", not "nothing
was reset"; `stats().aborted` is the counter that separates the two.

**`request_stop()`** asks a running server to shut down: it steps the lifecycle
state down and wakes the accept thread, without joining anything. It is the only
lifecycle call that is safe from a callback, where `stop()` would otherwise
deadlock on the thread running it. Reactors are not woken here, so one may take
up to `tick_ms` to notice; the controlling thread still has to call `stop()`,
which wakes and joins them.

**`wait()`** blocks while the server is running, polling every 200 ms. It
returns once the server has stopped — including when the accept thread failed,
or when the last live reactor failed; either sets `failed` and makes
`is_running()` false. A reactor that fails while another is still alive is
quarantined and does **not** make `wait()` return.

### Sending

```cpp
send_result send_to(uint64_t conn_id, const void* data, size_t len) noexcept;
```

Wraps `data` in a DATA frame and accepts it for sending. `len` must be ≤ 65536.

`ok` means **accepted**, not delivered: on the owning reactor the bytes are
already in the write buffer, elsewhere the frame is queued to the owner. A
shutdown, a reset, or a later write failure can still discard it — that is what
`bytes_dropped` counts.

| Result | Meaning |
|---|---|
| `ok` | Accepted: queued, or already in the write buffer. |
| `not_connected` | No such connection — never existed, or already torn down. |
| `closed` | The server is not running, or this connection is closing or closed. |
| `too_large` | `len > 65536`. |
| `overloaded` | The frame would exceed `max_pending_bytes`, the owner's queue is full, or the frame copy could not be allocated. |
| `invalid_argument` | `len > 0` with a null `data`. |

A `send_to` issued for a connection that is being closed returns `closed`
rather than `ok`, so a caller cannot queue a reply behind the FIN.

```cpp
bool close_connection(uint64_t conn_id) noexcept;
```

Starts a graceful close: pending output is drained, then FIN, then the peer's
FIN is awaited — all within the single `close_linger_ms` budget, counted from
the moment the close is parked. Returns `true` when the close was accepted:
queued to the owning reactor, or — when that queue was at its cap — recorded in
`close_wanted` for the tick sweep, which performs the same drain-then-close
within one tick. Returns `false` only when there is nothing to close: the
server is not running, the connection is unknown, or it has no owner. A refused
queue hand-off still increments `close_requests_dropped` even though it returns
`true`, because that counter is the queue-pressure signal, not a lost close.

A close answers what it promised and no more. What it serves is the input already
in the read buffer when the request is dequeued — the `close_input_bytes` budget
— plus, at most, one read round (64 KiB / 256 frames) pulled out of the kernel
receive buffer while that round runs. That round is exempt from the high-water
pause; the connection's ordinary continuation after it is not, so output
backpressure decides whether it gets to serve any more before the park. Requests
still in the kernel buffer past that are parsed and handed to the message
handler, but their replies are refused once the close parks, and the peer
receives a FIN over them. Measured
(`verify/evidence/probe_close_while_paused.cpp`): 4000 requests sitting in the
kernel of a read-paused connection, 256 answered, then FIN — while
`frames_recv` accounted for all 4000. Both numbers are real; only the second is
a promise. A connection that was read-paused is where the gap is widest, because
the pause exists precisely to leave bytes in the kernel.

```cpp
bool offload(uint64_t conn_id, std::function<void()> fn) noexcept;
```

Runs `fn` on the handler pool instead of the reactor. Tasks for one `conn_id`
run one at a time in submission order; different connections run in parallel.
`fn` runs off the reactor thread, so a `send_to` from it takes the queued path.
Returns `false` when no pool is configured or running, or when
`handler_queue_limit` queued tasks are already outstanding. A `true` return
means the task will run unless the server is stopped first — closing a
connection does not cancel its queued tasks, and a `send_to` from one then
returns `not_connected` or `closed`.

### Introspection

```cpp
uint16_t port() const noexcept;
size_t connection_count() const noexcept;
std::vector<uint64_t> connection_ids() const;
const server_stats& stats() const noexcept;
std::vector<reactor_stats_view> reactor_stats() const;
bool is_running() const noexcept;

struct reactor_stats_view {
    uint64_t frames_recv, frames_sent, bytes_recv, bytes_sent;
    size_t   connections;
    int64_t  last_progress_ms;
    bool     dead;
    bool     stalled;
    bool     retired;
};
```

`reactor_stats()` returns one entry per slot used since construction, in
reactor-index order; the vector is empty before the first `start()` allocates
the slots. An entry is current while its index is below the reactor count of
the latest successful start; surplus slots from an earlier, larger run stay in
the vector with `retired` set. A retired slot reports no per-run state but
keeps its cumulative traffic, so the four totals still add up to
`server_stats()`; after a start that failed after allocation every slot is
retired. The four traffic counters are cumulative since construction, the same
as `server_stats` (and stay cumulative across restarts). `connections` is the
count the reactor last published, reset to 0 by its teardown.
`last_progress_ms` is the steady-clock millisecond of its last completed loop
iteration, 0 until it runs and 0 for a retired slot. `dead` is set when the
reactor's loop failed and the reactor was quarantined: the slot takes no new
connections, its connections were reset with the `reactor failure` close
reason, and the server keeps serving on the remaining reactors. `stalled` is set
while the watchdog sees no progress for `reactor_stall_timeout_ms`: no new
connections are routed here, nothing is torn down, and the flag clears only
after a full window of progress; it is always false when the watchdog is off,
and quarantine clears it together with setting `dead`. Each field is read
atomically from a fixed slot array, so a snapshot can mix adjacent iterations;
it is not a transactional view. The result is a vector, so this
call can throw `bad_alloc`.

`port()` is the port actually bound: it equals `config::port` unless that was
`0`. It returns `0` before `start()` has bound and after `stop()` has closed.

`connection_count()` and `connection_ids()` take each shard's lock in turn and
are O(shards) plus the result size; `connection_ids()` and `reactor_stats()`
allocate. None of them is meant for a hot path.

`is_running()` is true only when the lifecycle reached running **and** no fatal
failure has been raised. A reactor failure is fatal only when no reactor is left
alive: the first failure quarantines that reactor, resets its connections and
lets the server keep serving, so `is_running()` remains true while at least one
reactor survives. The accept thread's failure, or the last live reactor's, is
fatal and clears the flag. Neither rewrites the lifecycle state, so a `start()`
verdict is never retroactively changed.

## `send_result`

```cpp
enum class send_result : int {
    ok = 0, not_connected, closed, too_large, overloaded, invalid_argument,
};
```

## `server_stats`

Read through `stats()`; every field is atomic and may be read at any time. The
four traffic counters are batched per reactor and published once per loop
iteration: exact when idle, up to one iteration behind under load.

| Field | Counted when |
|---|---|
| `accepted` | A reactor takes ownership of an accepted connection. |
| `rejected` | A socket is turned away before a reactor owns it: the admission ceiling was reached, the byte budget or the per-IP cap refused it, registration failed (`epoll_ctl`, a map insert, a declining `conn_state_handler`, an allocation failure), or every reactor was quarantined so none could take it. Not a capacity gauge on its own — pair it with `buffer_budget_rejections`, `ip_rejections`, `epoll_ctl_errors` and `accept_errors`. |
| `buffer_budget_rejections` | A connection was refused because `max_total_buffer_bytes` had no room for its buffers. |
| `ip_rejections` | A connection was refused because its peer address already holds `max_connections_per_ip` connections. |
| `closed` | A connection is fully torn down, whatever the reason. |
| `frames_recv` | A frame was parsed and consumed. A frame rejected from its header (bad magic, over-length, unknown type) is counted in `parse_errors` or `unknown_frame_types` instead. |
| `frames_sent` | A frame's last byte reaches the kernel — not when it is queued. |
| `bytes_recv` / `bytes_sent` | Bytes read from / written to sockets. |
| `queue_drops` | A whole frame is refused, either for exceeding `max_pending_bytes` or because a reactor queue is full. `send_task` submissions stop 64 slots short of the per-reactor cap, leaving that headroom for close/drain/park/new-connection tasks; frames refused there land here too. Control tasks refused at the full cap are counted here as well. |
| `bytes_dropped` | Bytes accepted for a connection but never sent: a frame dropped whole (capacity reject, or arriving after the close began), or output still buffered when the connection was torn down. |
| `aborted` | A connection ended with a reset rather than a graceful close. |
| `frames_dropped_on_close` | A frame arriving for a connection that is already closing, and therefore not answered. |
| `parse_errors` | A frame failed validation: bad magic or over-length. |
| `unknown_frame_types` | A header with a valid magic declared a type outside `1`/`2`/`3` while `allow_unknown_frame_types` was false, and the connection was closed from that header, before the declared payload arrived. Kept apart from `parse_errors` because the stream itself was intact. |
| `idle_timeouts` | No activity within `idle_timeout_ms`, where activity is bytes received or bytes drained to the peer. |
| `frame_assembly_timeouts` | A partial frame did not complete within `frame_assembly_timeout_ms`, even though bytes were arriving. |
| `epoll_ctl_errors` | An `epoll_ctl` registration failed. |
| `reactor_failures` | A reactor thread crashed or exited abnormally. That reactor is quarantined: it accepts no new connections, its connections are reset (close reason `reactor failure`), and the server keeps serving while another reactor is alive. The increment that empties the pool — or any accept-thread failure — is fatal. |
| `reactor_stalls` | The accept thread's watchdog observed a live reactor whose last completed loop iteration was older than `reactor_stall_timeout_ms`. One increment per episode, not per sample: the episode ends when the reactor has made progress for a full window again. Sampled at least every `min(500 ms, timeout/2)`; shorter transients can fall between samples, and a server that is no longer running is not sampled. The server is not stopped. |
| `reactor_stall_skips` | A new connection that would have gone to a stalled reactor went to a live one instead. Counted once per connection, not once per reactor passed over. Zero while the watchdog is off, and zero when every reactor is stalled: the accept loop then takes the bad hand-off rather than refusing the connection. |
| `handler_installs_refused` | `set_message_handler`, `set_conn_state_handler` or `set_close_handler` was called while the server was not fully stopped, or the install itself failed, and the handler was left unchanged. Handlers are read by every worker without synchronization, so installing one while they run is a data race rather than a reconfiguration. |
| `accept_errors` | `accept4` failed. `EMFILE` is answered with the reserved descriptor; `ENFILE` is not, and keeps the backoff. |
| `fd_exhausted_drops` | A connection was reset because the process ran out of descriptors and the spare was spent on it. The peer learns immediately instead of waiting in the backlog. |
| `fd_exhausted_system` | `accept4` failed with `ENFILE`: the system-wide descriptor table is full, which closing one's own spare cannot fix. |
| `read_pauses` | The reactor stopped reading a connection because its pending output passed the high watermark. |
| `pending_tasks_total` / `pending_tasks_depth` | Tasks submitted through the cross-thread queue, and the current queue depth. The owner-thread fast path touches neither. |
| `eof_reads` | `recv` returned 0. A parked half-close makes this repeat under level-triggered epoll, so a fast-moving value means the reactor is spinning on one FIN. |
| `close_requests_dropped` | The owning reactor refused the close hand-off. The close still happens — `do_tick` performs it — so this counter is a queue-pressure signal, not a lost close. |
| `epipe_writes` | `sendmsg` returned `EPIPE`. Expected to stay 0 — every path that could reach it is guarded first. |
| `close_callbacks_dropped` | The close notification could not be allocated; the connection was torn down unannounced. |
| `callback_exceptions` | A `conn_state_handler` or `close_handler` call threw. The exception is contained and the close proceeds. |

## Close reasons

The `reason` passed to `close_handler`, and why each one appears.

| Reason | Cause |
|---|---|
| `peer closed` | The peer sent FIN. Graceful if all output was already delivered. |
| `hup/err` | `EPOLLHUP`/`EPOLLERR` while the peer's FIN had not been read: the peer reset the connection. A server-initiated close that finishes with the peer's FIN does not report this. |
| `idle timeout` | No activity for `idle_timeout_ms`: no bytes received and no bytes drained to the peer. |
| `frame assembly timeout` | A partial frame did not complete in time. |
| `close linger expired` | The graceful close did not finish within `close_linger_ms`: either the FIN went unanswered, or the output ahead of it never drained. |
| `requested` | `close_connection` asked for a graceful close. |
| `server shutdown` | `stop()` tore the connection down. |
| `reactor failure` | The reactor that owned the connection failed and was quarantined; its connections are reset rather than drained. The server keeps serving if another reactor is alive. |
| `bad magic` | Frame header did not start with `0xEB90`. |
| `frame too large` | Declared length exceeded 65536. |
| `unknown frame type` | Type was not `1`/`2`/`3`, and `allow_unknown_frame_types` was false. |
| `rbuf full with incomplete frame` | The peer sent more data than the read buffer can hold without completing a frame. |
| `wbuf full on pong` | The write buffer could not take the PONG. |
| `scratch too small` | Internal invariant violation while copying a payload. |
| `recv error` / `send error` | `recv`/`sendmsg` failed. |
| `write side dead` | The socket reported its write side closed. |
| `wbuf short write` | A frame could not be copied into the write buffer in full — the capacity check above it should have made this impossible, so a partial frame is dropped rather than left in the stream. |
| `shutdown failed` | `shutdown(SHUT_WR)` failed while starting the FIN exchange. |
| `epoll write registration failed` | `EPOLL_CTL_MOD` for write interest failed. |
| `epoll read registration failed` | `EPOLL_CTL_MOD` to arm reading failed. |
| `epoll pause registration failed` | `EPOLL_CTL_MOD` to stop reading for backpressure failed. |
| `epoll resume registration failed` | `EPOLL_CTL_MOD` to resume reading failed. |
| `epoll park registration failed` | Re-arming interest on a parked half-close failed. |

## What the library does not do

- **No TLS and no authentication.** `bind_addr` defaults to every interface;
  putting this on an untrusted network means putting a TLS terminator or a
  firewall in front of it. The library's own admission controls are
  `max_connections`, `max_total_buffer_bytes` and `max_connections_per_ip`
  (the last one off by default) — resource limits, not a trust boundary.
- **No signal handling.** See [threading model](#threading-model).
- **No application-level framing beyond the header.** Payloads are opaque and
  are delivered exactly as framed.
- **Backpressure, not a flow-control protocol.** A peer that stops reading
  eventually fills `max_pending_bytes`; reads then pause at the high watermark
  until the drain falls to the low one, which lets TCP push back. The peer is
  never told a window: there is no credit scheme it could observe.
- **Keepalive is on**, with a 30 s idle interval, 10 s probe interval and 3
  probes, so a vanished peer is detected by the kernel even when
  `idle_timeout_ms` is long.
