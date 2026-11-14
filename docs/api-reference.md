# API reference

`include/tcp-server-mt.h` is the whole library: one header, no translation unit
to link, no third-party dependency. This document covers the surface it exposes
— the config struct, the server class, its three callbacks, `send_result`, and
`server_stats` — and the behaviour a caller is entitled to rely on. It applies
to revision `39fc5a9` (2026-11-07); the header is authoritative if the two
disagree.

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
- **Any other type value is delivered to the message handler** as if it were
  DATA. Type is not validated against the enum; applications that need their
  own types rely on this.
- A bad magic, an over-length frame, a ring buffer too small to hold the frame,
  or a received frame that cannot be answered (see [close
  reasons](#close-reasons)) ends the connection with a reset rather than a
  partial delivery.

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
| `backlog` | `1024` | `listen()` backlog. Must be > 0. |
| `reuse_port` | `false` | Set `SO_REUSEPORT` on the listener. Off by default so a duplicate instance fails to bind; on, the kernel may split connections between processes on the same port. |
| `num_reactors` | `0` | Reactor threads. `0` means `hardware_concurrency()`; capped at 64. |
| `max_connections` | `10000` | Admission ceiling. A connection over it is accepted and immediately closed, counted in `rejected`. |
| `max_total_buffer_bytes` | `0` | Ceiling on buffer bytes reserved by admitted connections. `0` means `max_connections × (rbuf_capacity + wbuf_capacity)`. Must admit at least one connection. |
| `max_connections_per_ip` | `0` | Most live connections from one numeric peer address. `0` is unlimited. Over the cap the socket is accepted and immediately closed, counted in `rejected` and `ip_rejections`. |
| `idle_timeout_ms` | `120000` | Close a connection after this long with **no bytes received**. Must be > 0. |
| `tick_ms` | `500` | Granularity of every deadline. Must be > 0; a tick coarser than a deadline makes that deadline late by up to one tick, and `start()` warns. |
| `frame_assembly_timeout_ms` | `0` | Deadline for one frame, measured from its first byte. `0` means 5 × `idle_timeout_ms`. Must be ≥ 0. |
| `rbuf_capacity` | `65544` | Per-connection read buffer. Must be ≥ 65544: a smaller one cannot hold a legal maximum frame. |
| `wbuf_capacity` | `65544` | Per-connection write buffer. Must be ≥ 8. |
| `max_pending_bytes` | `65544` | Ceiling on bytes accepted for one connection but not yet written. Clamped down to `wbuf_capacity` by `start()`. |
| `socket_send_buffer_bytes` | `0` | `SO_SNDBUF` for accepted sockets. `0` keeps the OS default; a smaller value bounds the kernel send-buffer memory one connection can pin and makes read backpressure engage sooner. |
| `read_high_watermark_bytes` | `0` | Above this much pending output the reactor stops reading and drops `EPOLLIN`. `0` means 3/4 of the effective `max_pending_bytes`. |
| `read_low_watermark_bytes` | `0` | Pending output at or below this resumes reading. `0` means half the high mark. Explicit values must satisfy `0 < low < high ≤ max_pending_bytes`. |
| `write_budget_bytes` | `262144` | Most bytes written to one socket in a single drain before control returns to the reactor, so one fast sender cannot monopolise it. Must be > 0. |
| `close_linger_ms` | `5000` | Total budget for a graceful close, counted from the moment it is parked: drain the write buffer, send the FIN, then wait for the peer's FIN. Must be > 0. |
| `handler_threads` | `0` | Size of the optional offload pool. `0` disables `offload()` entirely; capped at 64. |
| `handler_queue_limit` | `8192` | Queued offload tasks across the whole server. Must be > 0 when `handler_threads > 0`. |

Helpers resolve the "0 means decide for me" fields for callers that need the
same number the server used: `resolved_reactor_count()`,
`resolved_frame_assembly_timeout_ms()`,
`resolved_read_high_watermark_bytes()`,
`resolved_read_low_watermark_bytes()`,
`per_connection_buffer_bytes()` and `resolved_max_total_buffer_bytes()`.

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
`frame_assembly_timeout_ms < 0`, `handler_threads < 0`,
`handler_threads > 64`, `handler_threads > 0` with
`handler_queue_limit == 0`, `max_connections_per_ip < 0`,
`max_total_buffer_bytes` below one connection's buffers, or read watermarks that
are not `0 < low < high ≤ max_pending_bytes`.

`start()` also inspects `RLIMIT_NOFILE`: it tries to raise the soft limit to the
hard limit, and warns when the limit is below
`max_connections + 2 × (1 + reactors) + 16` — the accept epoll/eventfd pair,
one epoll/eventfd pair per reactor, and a small margin. When the limit is tight,
connections are still refused before `max_connections` is reached; each refusal
is counted in `accept_errors`.

**`stop()`** is immediate and abortive: stop accepting, close every connection,
join everything. Connections still holding queued output are torn down rather
than drained. Calling it twice is harmless. Called from a callback it only
requests shutdown and returns — the controlling thread must still call it.

**`stop_gracefully(timeout_ms)`** stops accepting, then asks every connection to
close gracefully and waits for up to `timeout_ms` (polling every 10 ms) before
falling through to `stop()`. Each connection is asked once; only a request the
reactor queue refused is retried. It returns `true` when no connections and no
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
returns once the server has stopped — including when a reactor failed, which
sets `failed` and makes `is_running()` false.

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
the moment the close is parked. Returns `true` if the request reached the owning
reactor's queue immediately, `false` if it did not — the server is not running,
the connection is unknown, it has no owner, or the queue refused it. A refused
request is **not lost**: it is recorded on the connection and `do_tick()`'s
sweep performs the same drain-then-close within one tick, so `false` means
"delayed", not "ignored". Each refusal is counted in `close_requests_dropped`.

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
bool is_running() const noexcept;
```

`port()` is the port actually bound: it equals `config::port` unless that was
`0`. It returns `0` before `start()` has bound and after `stop()` has closed.

`connection_count()` and `connection_ids()` take each shard's lock in turn and
are O(shards) plus the result size; `connection_ids()` allocates. Neither is
meant for a hot path.

`is_running()` is true only when the lifecycle reached running **and** no
component has failed. A failed reactor or accept thread clears it without
rewriting the lifecycle state, so a `start()` verdict is never retroactively
changed.

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
| `rejected` | A socket is turned away before a reactor owns it: the admission ceiling was reached, or registration failed (`epoll_ctl`, a map insert, a declining `conn_state_handler`, an allocation failure). Not a capacity gauge on its own — pair it with `epoll_ctl_errors` and `accept_errors`. |
| `closed` | A connection is fully torn down, whatever the reason. |
| `frames_recv` | A frame is parsed. |
| `frames_sent` | A frame's last byte reaches the kernel — not when it is queued. |
| `bytes_recv` / `bytes_sent` | Bytes read from / written to sockets. |
| `queue_drops` | A whole frame is refused, either for exceeding `max_pending_bytes` or because a reactor queue is full. Control tasks refused by the same queues are counted here too. |
| `bytes_dropped` | Bytes accepted for a connection but never sent: a frame dropped whole (capacity reject, or arriving after the close began), or output still buffered when the connection was torn down. |
| `aborted` | A connection ended with a reset rather than a graceful close. |
| `frames_dropped_on_close` | A frame arriving for a connection that is already closing, and therefore not answered. |
| `parse_errors` | A frame failed validation: bad magic or over-length. |
| `idle_timeouts` | No bytes arrived within `idle_timeout_ms`. |
| `frame_assembly_timeouts` | A partial frame did not complete within `frame_assembly_timeout_ms`, even though bytes were arriving. |
| `epoll_ctl_errors` | An `epoll_ctl` registration failed. |
| `reactor_failures` | A reactor thread crashed or exited abnormally. |
| `accept_errors` | `accept4` failed — typically descriptor exhaustion. |
| `pending_tasks_total` / `pending_tasks_depth` | Tasks submitted through the cross-thread queue, and the current queue depth. The owner-thread fast path touches neither. |
| `eof_reads` | `recv` returned 0. A parked half-close makes this repeat under level-triggered epoll, so a fast-moving value means the reactor is spinning on one FIN. |
| `close_requests_dropped` | The queue refused `close_connection`'s immediate hand-off. The close is still performed by the tick sweep; the counter is a queue-pressure signal. |
| `read_pauses` | Read backpressure engaged: pending output crossed the high watermark and `EPOLLIN` was dropped. |
| `buffer_bytes` / `buffer_budget_rejections` | Buffer bytes reserved by admitted connections, and admissions refused because `max_total_buffer_bytes` had no room. |
| `ip_rejections` | Admissions refused because the peer address already had `max_connections_per_ip` live connections. |
| `epipe_writes` | `sendmsg` returned `EPIPE`. Expected to stay 0 — every path that could reach it is guarded first. |
| `close_callbacks_dropped` | The close notification could not be allocated; the connection was torn down unannounced. |
| `callback_exceptions` | A close-side callback threw. The exception was contained; the connection was already gone. |

## Close reasons

The `reason` passed to `close_handler`, and why each one appears.

| Reason | Cause |
|---|---|
| `peer closed` | The peer sent FIN. Graceful if all output was already delivered. |
| `hup/err` | `EPOLLERR`: the peer reset the connection. Also `EPOLLHUP` when no readable EOF accompanied it. A plain `EPOLLHUP` after our own FIN is the peer's FIN and is reported as `peer closed`. |
| `idle timeout` | No bytes for `idle_timeout_ms`. |
| `frame assembly timeout` | A partial frame did not complete in time. |
| `close linger expired` | The graceful close did not finish within `close_linger_ms`: either the FIN went unanswered, or the output ahead of it never drained. |
| `server shutdown` | `stop()` tore the connection down. |
| `bad magic` | Frame header did not start with `0xEB90`. |
| `frame too large` | Declared length exceeded 65536. |
| `rbuf full with incomplete frame` | The peer sent more data than the read buffer can hold without completing a frame. |
| `pending bytes at limit on pong` | A PING arrived while the connection had no credit left for the PONG. |
| `wbuf full on pong` | The write buffer could not take the PONG. |
| `scratch too small` | Internal invariant violation while copying a payload. |
| `recv error` / `send error` | `recv`/`sendmsg` failed. |
| `write side dead` | The socket reported its write side closed. |
| `wbuf short write` | A frame could not be copied into the write buffer in full — the capacity check above it should have made this impossible, so a partial frame is dropped rather than left in the stream. |
| `shutdown failed` | `shutdown(SHUT_WR)` failed while starting the FIN exchange. |
| `epoll write registration failed` | `EPOLL_CTL_MOD` for write interest failed. |
| `epoll read registration failed` | `EPOLL_CTL_MOD` to resume reading failed. |
| `epoll park registration failed` | Re-arming interest on a parked half-close failed. |

## What the library does not do

- **No TLS and no authentication.** `bind_addr` defaults to every interface;
  putting this on an untrusted network means putting a TLS terminator or a
  firewall in front of it. Per-IP admission is available but opt-in
  (`max_connections_per_ip`).
- **No signal handling.** See [threading model](#threading-model).
- **No application-level framing beyond the header.** Payloads are opaque and
  are delivered exactly as framed.
- **No application-level pacing beyond backpressure.** A peer that stops reading
  crosses the read high watermark, at which point the reactor stops reading so
  TCP pushes back on the sender; `send_to` still refuses frames past
  `max_pending_bytes`.
- **Keepalive is on**, with a 30 s idle interval, 10 s probe interval and 3
  probes, so a vanished peer is detected by the kernel even when
  `idle_timeout_ms` is long.
