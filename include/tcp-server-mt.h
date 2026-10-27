// industrial_tcp_server_mt.hpp
// Multi-threaded reactor, whole-frame buffering, RAII, explicit close protocol
// C++17 / Linux (epoll + eventfd)
#pragma once

#ifndef __linux__
#error "requires Linux (epoll + eventfd)"
#endif

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cassert>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>
#include <array>
#include <shared_mutex>

namespace industrial {

// ==================== Frame protocol ====================
constexpr uint16_t FRAME_MAGIC       = 0xEB90;
constexpr size_t   FRAME_HEADER_SIZE = 8;
constexpr uint32_t MAX_FRAME_PAYLOAD = 64 * 1024;
// The largest frame that can appear on the wire: one header plus the maximum
// payload. Buffers that must hold any legal frame are sized from this rather
// than from a rounded number that happens to be close.
constexpr size_t   MAX_FRAME_SIZE = FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD;

enum class frame_type : uint16_t {
    DATA = 0x0001,
    PING = 0x0002,
    PONG = 0x0003,
};

inline int64_t now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// The header is eight bytes written big-endian, and from here on only this one
// place writes it: keeping two copies of the same byte orderings around is how
// they drift apart.
inline void write_frame_header(uint8_t* out, frame_type type,
                               size_t len) noexcept {
    const uint16_t t = static_cast<uint16_t>(type);
    out[0] = (FRAME_MAGIC >> 8) & 0xFF;
    out[1] =  FRAME_MAGIC       & 0xFF;
    out[2] = (t >> 8) & 0xFF;
    out[3] =  t       & 0xFF;
    out[4] = (len >> 24) & 0xFF;
    out[5] = (len >> 16) & 0xFF;
    out[6] = (len >>  8) & 0xFF;
    out[7] =  len        & 0xFF;
}

inline const std::array<uint8_t, FRAME_HEADER_SIZE>& pong_frame() noexcept {
    static const std::array<uint8_t, FRAME_HEADER_SIZE> k_pong = [] {
        std::array<uint8_t, FRAME_HEADER_SIZE> v{};
        write_frame_header(v.data(), frame_type::PONG, 0);
        return v;
    }();
    return k_pong;
}

inline void encode_frame_into(uint8_t* out, frame_type type,
                            const void* payload, size_t len) noexcept {
    // A const char* source is assumed. len <= MAX_FRAME_PAYLOAD is enforced by
    // send_to, but this is an inline public entry point and nothing here would
    // complain about a caller who did not check: it would just write a length
    // the far side cannot honour.
    assert(len <= MAX_FRAME_PAYLOAD);
    write_frame_header(out, type, len);
    if (len > 0 && payload) std::memcpy(out + FRAME_HEADER_SIZE, payload, len);
}

// ==================== RAII fd ====================
struct fd_guard {
    int fd = -1;
    fd_guard() noexcept = default;
    explicit fd_guard(int f) noexcept : fd(f) {}
    fd_guard(fd_guard&& o) noexcept : fd(o.fd) { o.fd = -1; }
    fd_guard& operator=(fd_guard&& o) noexcept {
        if (this != &o) {
            reset(); fd = o.fd; o.fd = -1;
        }
        return *this;
    }
    fd_guard(const fd_guard&) = delete;
    fd_guard& operator=(const fd_guard&) = delete;
    ~fd_guard() { reset(); }
    void reset(int f = -1) noexcept {
        if (fd >= 0) {
            ::close(fd);
        }
        fd = f;
    }
    int  release() noexcept { int f = fd; fd = -1; return f; }
    explicit operator bool() const noexcept { return fd >= 0; }
};

// ==================== Ring buffer ====================
class ring_buffer {
 public:
    ring_buffer() noexcept = default;
    explicit ring_buffer(size_t cap) { init(cap); }

    // May throw bad_alloc; the caller chooses the policy (once per connection,
    // at setup).
    // A connection is default-constructed and initialised a few lines later,
    // so for a short while cap_ really is 0. Every `+= N % cap_` below is UB
    // for that value -- integer division by zero, which is a trap rather than
    // a wrong answer -- so it is rejected here instead of being argued about
    // per method.
    void init(size_t cap) {
        // resizing over live data would drop it silently
        assert(empty());
        assert(cap > 0);
        buf_.resize(cap);
        cap_  = cap;
        head_ = 0;
        size_ = 0;
    }

    size_t capacity()  const noexcept { return cap_; }
    size_t readable()  const noexcept { return size_; }
    size_t writable()  const noexcept { return cap_ - size_; }
    bool   empty()     const noexcept { return size_ == 0; }
    bool   full()      const noexcept { return size_ == cap_; }

    // Writes what fits and returns how much landed. A caller that wants a whole
    // frame written checks writable() >= n itself.
    size_t write(const uint8_t* p, size_t n) noexcept {
        if (cap_ == 0) return 0;   // uninitialised: see init()
        size_t w = std::min(n, writable());
        if (w == 0) return 0;
        size_t tail  = write_tail();
        size_t first = std::min(w, cap_ - tail);
        std::memcpy(buf_.data() + tail, p, first);
        if (w > first) std::memcpy(buf_.data(), p + first, w - first);
        size_ += w;
        return w;
    }

    size_t peek(uint8_t* p, size_t n) const noexcept {
        size_t r = std::min(n, size_);
        if (r == 0) return 0;
        size_t first = std::min(r, cap_ - head_);
        std::memcpy(p, buf_.data() + head_, first);
        if (r > first) std::memcpy(p + first, buf_.data(), r - first);
        return r;
    }

    // Same as peek, but starting `skip` bytes into the readable region. The
    // send path uses it to read a frame header that is not at the head.
    size_t peek_at(size_t skip, uint8_t* p, size_t n) const noexcept {
        if (skip >= size_) return 0;
        size_t r = std::min(n, size_ - skip);
        size_t at = (head_ + skip) % cap_;
        size_t first = std::min(r, cap_ - at);
        std::memcpy(p, buf_.data() + at, first);
        if (r > first) std::memcpy(p + first, buf_.data(), r - first);
        return r;
    }

    // Room at the write end, for a caller that fills the buffer in place
    // instead of copying through a stack buffer first. Only the contiguous part
    // is offered; a ring that has wrapped has a second piece at buf_[0].
    uint8_t* write_ptr() noexcept {
        if (cap_ == 0) return nullptr;
        return buf_.data() + write_tail();
    }
    size_t write_span() const noexcept {
        if (cap_ == 0) return 0;
        const size_t tail = write_tail();
        return std::min(cap_ - size_, cap_ - tail);
    }
    // Marks n bytes written through write_ptr(). Clamped to the span that was
    // offered, not to writable(): bytes past the end of the ring were never
    // written contiguously, and claiming them readable would expose stale data.
    void commit(size_t n) noexcept { size_ += std::min(n, write_span()); }

    void consume(size_t n) noexcept {
        size_t r = std::min(n, size_);
        if (r == 0) return;    // also covers the uninitialised cap_ == 0 case
        head_ = (head_ + r) % cap_;
        size_ -= r;
    }

    struct spans_view {
        const uint8_t* p1 = nullptr; size_t n1 = 0;
        const uint8_t* p2 = nullptr; size_t n2 = 0;
        int count = 0;
    };
    spans_view spans() const noexcept {
        spans_view s;
        if (size_ == 0) return s;
        size_t first = std::min(size_, cap_ - head_);
        s.p1 = buf_.data() + head_;
        s.n1 = first;
        if (size_ > first) {
            s.p2 = buf_.data(); s.n2 = size_ - first; s.count = 2;
        }
        else s.count = 1;
        return s;
    }

    void clear() noexcept { head_ = 0; size_ = 0; }

 private:
    // Offset of the write end. Defined as 0 for the uninitialised ring; every
    // public method checks cap_ == 0 before it gets here.
    size_t write_tail() const noexcept {
        return cap_ == 0 ? 0 : (head_ + size_) % cap_;
    }

    std::vector<uint8_t> buf_;
    size_t cap_  = 0;
    size_t head_ = 0;
    size_t size_ = 0;
};

// ==================== Statistics ====================
struct server_stats {
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> rejected{0};
    std::atomic<uint64_t> closed{0};
    // Counted when a frame is parsed. Reactors batch the count in a local
    // line-aligned tally and publish it once per loop iteration (and once on
    // loop exit), so it is exact at idle and can lag one iteration while
    // traffic flows.
    alignas(64) std::atomic<uint64_t> frames_recv{0};
    // Counted when a frame's last byte reaches the kernel
    // (drain_wbuf_to_socket), the same point as bytes_sent. Counted on entering
    // wbuf instead, frames an abort threw away would sit in the total forever
    // with nowhere to deduct them -- in one measured abort the counter claimed
    // 511 frames while 48 had actually reached the peer. Reactors batch it in a
    // local line-aligned tally and publish it once per loop iteration (and once
    // on loop exit), so it is exact at idle and can lag one iteration while
    // traffic flows.
    alignas(64) std::atomic<uint64_t> frames_sent{0};
    // Batched the same way: each reactor accumulates locally and publishes once
    // per loop iteration.
    alignas(64) std::atomic<uint64_t> bytes_recv{0};
    alignas(64) std::atomic<uint64_t> bytes_sent{0};
    alignas(64) std::atomic<uint64_t> queue_drops{0};  // whole frames refused
    // bytes still unsent when the connection closed
    std::atomic<uint64_t> bytes_dropped{0};
    // forced disconnects (RST), not a graceful close
    std::atomic<uint64_t> aborted{0};
    // frames dropped mid-close; the FIN is out
    std::atomic<uint64_t> frames_dropped_on_close{0};
    std::atomic<uint64_t> parse_errors{0};
    std::atomic<uint64_t> idle_timeouts{0};
    // A partial frame did not complete within frame_assembly_timeout_ms. Unlike
    // idle_timeouts, this fires while bytes are still arriving.
    std::atomic<uint64_t> frame_assembly_timeouts{0};
    std::atomic<uint64_t> epoll_ctl_errors{0};
    // reactor threads that crashed or exited abnormally
    std::atomic<uint64_t> reactor_failures{0};
    // accept4 failed for want of a file descriptor. The one accept failure
    // predictable at startup, and therefore the one that has to be countable:
    // without it the only symptom is that it stopped accepting connections and
    // never said a word about it.
    std::atomic<uint64_t> accept_errors{0};
    // Slow-path task counters. The owner-thread send_to() fast path does not
    // queue, so it does not touch either.
    alignas(64) std::atomic<uint64_t> pending_tasks_total{0};
    alignas(64) std::atomic<uint64_t> pending_tasks_depth{0};  // queue gauge
    // recv returned 0 (peer's FIN). Under level-triggered epoll a peer that has
    // half-closed makes epoll_wait hand back the same EPOLLIN every time, so
    // this doubles as a probe for whether the reactor is spinning on one FIN:
    // across a parked drain it should barely move.
    std::atomic<uint64_t> eof_reads{0};
    // close_connection() sent, then refused by the queue. Each of those closes
    // never reached a connection and nothing counts it; what waits for them is
    // the idle timeout.
    std::atomic<uint64_t> close_requests_dropped{0};
    // sendmsg returned EPIPE: some socket's write side has been judged dead.
    // Every path that could reach it is intercepted by a guard first (after our
    // own shutdown, both handle_write and park_close's fin_sent check return
    // before it; a peer RST takes the EPOLLERR/EPOLLHUP branch in loop()
    // first), so it is expected to sit at 0 -- and that is what this counter is
    // for: it keeps "unreachable" something you can read rather than a claim.
    // If it ever moves, the reasoning above is void and this path needs another
    // look.
    std::atomic<uint64_t> epipe_writes{0};
    // Close callbacks dropped: the connection was torn down completely, but the
    // allocation that was to notify the application failed. Expected to stay 0
    // (nothing but bad_alloc reaches it), and it exists because losing a
    // notification has to be readable -- otherwise the only symptom is a peer
    // watching the connection vanish with its callback never invoked and no
    // counter moving at all.
    std::atomic<uint64_t> close_callbacks_dropped{0};
};

// ==================== Send results ====================
enum class send_result : int {
    ok = 0,
    not_connected,   // no such connection
    closed,         // connection closed, or server stopped
    too_large,       // payload exceeds MAX_FRAME_PAYLOAD
    overloaded,     // logical limit reached, or queue full
    invalid_argument,
};

class multi_reactor_server;
class sub_reactor;

// One reservation follows a socket from the accept queue through connection
// lifetime. Every failure/discard path releases it through the same destructor.
// The RMWs are relaxed, same policy as every other gauge: RMW cannot lose an
// update, nothing non-atomic is published through the count, and the accept
// loop's cap check is advisory by nature (the load races the reservation by
// definition). The ++/-- operator forms would quietly mean seq_cst.
struct admission_ticket {
    std::atomic<size_t>& count;
    explicit admission_ticket(std::atomic<size_t>& value) noexcept
        : count(value) {
        count.fetch_add(1, std::memory_order_relaxed);
    }
    admission_ticket(const admission_ticket&) = delete;
    admission_ticket& operator=(const admission_ticket&) = delete;
    admission_ticket(admission_ticket&&) = delete;
    admission_ticket& operator=(admission_ticket&&) = delete;
    ~admission_ticket() { count.fetch_sub(1, std::memory_order_relaxed); }
};

// ==================== Connection ====================
struct connection {
    uint64_t    id    = 0;
    int         fd    = -1;
    std::string peer;
    sub_reactor* owner = nullptr;

    // Owned by the reactor thread; nothing below here is touched from
    // elsewhere.
    //
    // Last time a byte arrived from this peer, not last time a frame was
    // completed: an idle deadline is a promise about silence, and a peer
    // dribbling one large frame over a slow link is not silent. Stamped only
    // where bytes arrive (handle_read); parsing does not touch it, so a frame
    // that completes late cannot extend the deadline.
    // Consequence: a peer that sends bytes which never complete a frame is now
    // never idle-reclaimed. It is bounded by rbuf (a frame larger than
    // rbuf_capacity is closed as incomplete) and by TCP keepalive, not by the
    // idle tick.
    int64_t    last_recv_ms = 0;
    // When the first byte of the frame currently being assembled arrived; 0
    // means no partial frame. Later bytes do not refresh it: this deadline is
    // about progress, not silence. Reactor-thread-only.
    int64_t    frame_started_at_ms = 0;
    ring_buffer rbuf;
    ring_buffer wbuf;
    // Target state of this socket's EPOLLOUT bit, owned by the worker thread.
    // arm_write/disarm_write are idempotent transitions toward it, never blind
    // epoll_ctl calls, so a stale EPOLLOUT event cannot tear down a live write
    // interest (and a queued frame cannot be stranded without a wakeup).
    bool       write_armed = false;
    // Bytes of the frame at the head of wbuf that have not reached the kernel.
    // wbuf only ever receives whole frames, so the head is either inside
    // exactly one frame or exactly at a frame boundary; this field says which,
    // so the drain can count a frame when its last byte leaves instead of when
    // it is queued. Reactor-thread-only, like wbuf itself.
    size_t      send_frame_remain = 0;
    // -------- close protocol --------
    // closing: a graceful close has been requested and is being honoured
    // (output
    //          drained, FIN sent or imminent). Submissions are refused from
    //          here
    //          on, but the peer keeps reading what we owe it.
    // fin_sent: FIN has actually left. Closing before that would drop queued
    // bytes
    //          and turn the polite close into the RST this whole mechanism
    //          avoids.
    // The two are separate on purpose: "closed" must not mean "socket is gone",
    // or an application that sends a reply and then closes would never get the
    // reply out. Reads keep running throughout.
    // Atomic, though every write is reactor-thread-only: send_to() reads it,
    // and send_to runs on whichever thread the application called it from.
    // The two have to agree, because once this is set the reactor refuses
    // every frame (see submit_send) -- send_to answering ok to one would
    // promise something the owner has already decided not to do.
    std::atomic<bool> closing{false};
    // A close request has been queued but the owner may not have parked it yet.
    // The owner-thread send_to() fast path falls back to the queued path when
    // this is set, so a send issued after close_connection() keeps the existing
    // queue order (and its existing drop) instead of overtaking it.
    std::atomic<bool> close_requested{false};
    bool       fin_sent = false;
    // A graceful close whose final read dispatched requests still has their
    // replies queued, and the FIN waits for them. At most one queue round may
    // be spent waiting -- a peer that keeps sending cannot hold the close open
    // with more input -- and the round has an owner, because only the side that
    // deferred knows what the wait was for:
    //
    //   close       -- drain_input_then_close() deferred; the round ends in a
    //                  park.
    //   peer_closed -- finish_peer_closed() deferred; the round ends in
    //                  whatever finish_peer_closed() decides once the replies
    //                  have landed in wbuf -- a FIN, or "nothing left, close
    //                  now".
    //
    // One bool cannot say which, and conflating them cost the peer its last
    // answer: taken as "the round has been spent, park now" rather than "the
    // round is still owed", it parked the close one round early, with wbuf
    // still empty and the FIN therefore immediate -- over replies whose own
    // deferral had not run yet.
    enum class deferred_round_kind : uint8_t { none = 0, close, peer_closed };
    deferred_round_kind deferred_round = deferred_round_kind::none;
    // Input this graceful close still owes the peer: the bytes that were
    // buffered and unserved when the close was first requested.
    // drain_input_then_close() may spend queue rounds on them, because
    // handle_read stops at a fixed per-call budget and one pass cannot be
    // assumed to have served a whole backlog -- but on nothing beyond them, so
    // a peer that keeps sending cannot hold the close open with fresh input.
    // Negative means "not measured yet". Reactor-thread-only.
    int64_t    close_input_bytes = -1;
    // Bytes this connection's handle_read() has taken off the socket since
    // drain_input_then_close() zeroed it, which is always either nobody or the
    // graceful close: it is how a round can be charged for input that arrived
    // *during* the round. Reactor-thread-only, like close_input_bytes.
    size_t     drain_read_bytes = 0;
    // peer's FIN has been seen (recv returned 0). Set once, in handle_read;
    // handle_write consults it to finish a parked close deterministically the
    // moment the drain completes, instead of waiting for a spurious EPOLLIN.
    bool       peer_closed = false;
    int64_t    close_started_at_ms = 0;   // when the graceful close was parked
    int64_t    fin_sent_at_ms = 0;        // when shutdown(SHUT_WR) returned

    // Shared across threads from here down:
    std::atomic<bool>   is_closed{false};
    // admission credit for output: bytes reserved and not yet handed to the
    // kernel, whether they are still sitting in a queued send task or already
    // sitting in wbuf. It is one number covering both because neither one alone
    // says whether the peer is being kept up with.
    //
    // Lifecycle, which has to be read as the three places it is touched rather
    // than the one place it happens to stay level:
    //   1. reserve -- enqueueing a frame adds its length (PONG included);
    //   2. migrate -- moving a frame from a queued task into wbuf changes
    //                 nothing, and that is the only reason some paths nearby
    //                 do not visibly adjust it;
    //   3. release -- subtracting what was actually written, what was dropped
    //                 whole (capacity reject, closing connection, refused
    //                 PONG), and what was still unsent when we tore down.
    //
    // Every release runs on the connection's owning reactor, so release/release
    // never races. Reserving races between application threads and that same
    // reactor's PONG path, and relaxed is enough for all of it: nothing
    // non-atomic is published through this counter, the frame itself crosses
    // under pending_mutex_, and RMW already guarantees no update is lost --
    // which is the property that matters, because a lost decrement is a
    // permanent leak and a visible one in any watermark built on top.
    // Ordering past that would only buy freshness, and freshness is not
    // something this number can promise anyway: it is a best-effort ceiling,
    // so reading a stale value costs one refused frame, never correctness.
    std::atomic<size_t> pending_bytes{0};
    // Worker-owned continuation after a parsing budget.
    bool read_pending = false;
    connection* read_next = nullptr;
    connection* read_prev = nullptr;
    std::shared_ptr<admission_ticket> admission;

    ~connection() noexcept {
        // Backstop: close at most once. The normal path already leaves fd at
        // -1.
        if (fd >= 0) {
            ::close(fd); fd = -1;
        }
    }
};

// How far a drain got. These used to be 0 / 1 / -1 with two different outcomes
// sharing the 0: the while-loop ending because wbuf was empty, and EPIPE. The
// first means every queued byte reached the kernel and the caller should go on
// to whatever comes next; the second means not one more byte will ever leave,
// and treating them alike is what put the connection's fate in the hands of
// `wbuf.empty()` at each call site -- a fact none of them said out loud.
enum class drain_outcome {
    drained,   // wbuf is empty: everything owed has reached the kernel
    stalled,   // fair-share budget spent, or EAGAIN: EPOLLOUT will resume it
    dead,      // EPIPE: this socket's write direction is finished, permanently
    error,     // any other failure: fatal
};

// One closed connection, held until its close callback has run. The conn is
// kept here on purpose: a deferred callback must not observe a connection that
// has already been destroyed, and peer/reason are copied out because the
// caller's `reason` is borrowed for that one call.
struct closed_conn {
    std::shared_ptr<connection> conn;
    uint64_t              id     = 0;
    std::string           peer;
    std::string           reason;
};

// ==================== sub_reactor ====================
class sub_reactor {
 public:
    sub_reactor(int idx, multi_reactor_server* server) noexcept
        : idx_(idx), server_(server) {}
    ~sub_reactor() noexcept { stop(); }

    sub_reactor(const sub_reactor&) = delete;
    sub_reactor& operator=(const sub_reactor&) = delete;

    bool start() noexcept;
    void stop()  noexcept;

    // Submit through the server's lifetime barrier. New-connection submission
    // always consumes its fd_guard; failed sends require a byte-reservation
    // rollback.
    bool submit_new_connection(fd_guard fd, std::string peer, uint64_t id,
                               std::shared_ptr<admission_ticket> admission)
        noexcept;
    bool submit_send(uint64_t id, std::vector<uint8_t> frame) noexcept;
    bool submit_close(uint64_t id) noexcept;

    // The reactor thread this call is running on, if any. send_to() uses it for
    // the owner-thread fast path; application and accept threads see null.
    static sub_reactor* current() noexcept { return current_; }
    // Try to write a DATA frame straight into the connection's wbuf. Only valid
    // on the owning reactor thread. expected is the server send_to() was called
    // on: two servers in one process share reactor threads and connection ids,
    // so a mismatch must take the slow path, not this map.
    // Returns false when the id is not in this reactor's map, or when wbuf has
    // no room right now (the caller then takes the queued path, which owns the
    // backpressure accounting).
    bool try_send_local(multi_reactor_server* expected, uint64_t id,
                      const void* data, size_t len,
                      send_result* out) noexcept;

    int idx() const noexcept { return idx_; }

 private:
    inline static thread_local sub_reactor* current_ = nullptr;
    void loop() noexcept;
    void wakeup() noexcept;
    void drain_wakeup() noexcept;
    void process_pending() noexcept;
    // Returns the number of frames this call handed to the message handler.
    // Zero means the read produced no work at all, which
    // drain_input_then_close() uses to tell "the peer asked for nothing more"
    // from "answers are queued behind us".
    int  handle_read(connection* c) noexcept;
    void handle_write(connection* c) noexcept;
    void parse_frames(connection* c, int& frames_left) noexcept;
    // graceful=true: whatever we still owe the peer is delivered before the
    // FIN, and the socket is closed only after the peer's own FIN (or
    // close_linger_ms). Use it whenever the peer is a legitimate counterparty
    // that should not lose its last reply. graceful=false: abort now -- pending
    // output is dropped and an RST is requested. For protocol violations,
    // resource exhaustion and fault paths, where delivering a half-processed
    // stream would be worse than the reset.
    // Default graceful=true: see the definition for what the two modes mean.
    void close_conn(connection* c, const char* reason,
                    bool graceful = true) noexcept;
    void teardown_closed(connection* c, const char* reason,
                         bool graceful) noexcept;
    void park_close(connection* c, const char* reason) noexcept;
    bool try_begin_write_shutdown(connection* c) noexcept;
    // A graceful close asked for while the peer's request is still unread:
    // reads and answers it before the FIN goes out. See the definition for why
    // the FIN has to wait a queue round, and why it waits at most one.
    void drain_input_then_close(connection* c) noexcept;
    // Precondition: parse_frames has already drained every complete frame, so
    // any reply it triggered is queued ahead of the FIN this may send.
    // handle_read establishes that before calling in; it is not checked here.
    void finish_peer_closed(connection* c) noexcept;
    void unregister_conn(connection* c, bool graceful = true) noexcept;
    // The rest of a rollback for a connection that never took its place here.
    void discard_rejected_conn(connection* c) noexcept;
    void discard_pending_output(connection* c) noexcept;
    void finish_close(std::shared_ptr<connection>& c) noexcept;
    void flush_close_callbacks() noexcept;
    void arm_write(connection* c) noexcept;
    void disarm_write(connection* c) noexcept;
    void do_tick() noexcept;
    drain_outcome drain_wbuf_to_socket(connection* c) noexcept;
    // Typed queue tasks. Sends and closes are inline (no std::function
    // allocation); a new connection is rare enough to keep behind a unique_ptr,
    // which also lets its fd_guard stay move-only.
    struct new_conn_task {
        fd_guard   fd;
        std::string peer;
        uint64_t  id = 0;
        std::shared_ptr<admission_ticket> admission;
    };
    struct send_task {
        uint64_t id = 0;
        std::vector<uint8_t> frame;
    };
    struct close_task { uint64_t id = 0; };
    struct finish_peer_closed_task { uint64_t id = 0; };
    struct drain_input_task { uint64_t id = 0; };
    struct park_close_task { uint64_t id = 0; };
    using queue_task = std::variant<std::unique_ptr<new_conn_task>, send_task,
                                    close_task, finish_peer_closed_task,
                                    drain_input_task, park_close_task>;
    bool enqueue(queue_task task) noexcept;
    void handle_task(std::unique_ptr<new_conn_task>& task) noexcept;
    void handle_task(send_task& task) noexcept;
    void handle_task(close_task& task) noexcept;
    void handle_task(finish_peer_closed_task& task) noexcept;
    void handle_task(drain_input_task& task) noexcept;
    void handle_task(park_close_task& task) noexcept;
    using task_dispatch = void (*)(sub_reactor*, queue_task&) noexcept;
    using dispatch_table =
        std::array<task_dispatch, std::variant_size_v<queue_task>>;
    template <size_t I>
    static void dispatch_task(sub_reactor* self, queue_task& task) noexcept {
        self->handle_task(std::get<I>(task));
    }
    // Built from queue_task's alternative list rather than written out by hand:
    // a task type without a handle_task overload fails to compile, and a
    // missing or reordered slot cannot happen.
    template <size_t... I>
    static constexpr std::array<task_dispatch, sizeof...(I)>
    make_task_dispatch(std::index_sequence<I...>) noexcept {
        return std::array<task_dispatch, sizeof...(I)>{
            {&dispatch_task<I>...}};
    }
    // Defined after the class: GCC will not instantiate this member-function
    // template from an in-class initializer.
    static const dispatch_table k_task_dispatch;
    void cleanup() noexcept;
    // Publish the per-reactor traffic counters to the shared stats, then reset
    // them. Called once per loop iteration and once on loop exit, so each
    // counter is exact at idle and can lag one iteration while traffic flows.
    void publish_stats() noexcept;
    void schedule_read(connection* c) noexcept;
    void unschedule_read(connection* c) noexcept;
    connection* read_head_ = nullptr;
    connection* read_tail_ = nullptr;

    // Per-reactor traffic counters, each on its own cache line and published by
    // publish_stats() once per loop iteration (and once on exit). Publishing
    // the shared atomic once per frame was 18.5% slower than removing the
    // counter entirely (2198 -> 2605 MB/s); batching recovers nearly all of
    // that gain. The alignment is not decoration: a counter sharing a line with
    // the reactor's other hot fields bounces at a different address, which is
    // what the first attempt did. idx_ is aligned too so the last counter is
    // alone on its line.
    alignas(64) uint64_t frames_recv_local_ = 0;
    alignas(64) uint64_t frames_sent_local_ = 0;
    alignas(64) uint64_t bytes_recv_local_ = 0;
    alignas(64) uint64_t bytes_sent_local_ = 0;

    alignas(64) int      idx_;
    multi_reactor_server*  server_;

    int               epoll_fd_ = -1;
    int               wake_fd_  = -1;
    // Worker-owned: false means this reactor accepts no more submissions.
    std::atomic<bool> running_{false};
    std::thread       thread_;

    // Queue cap, and it is **per reactor** -- which is a different number from
    // "per server" in a way that matters. pick_reactor() spreads submissions
    // out, so a broadcast to 8k connections across 4 reactors asks each queue
    // for 2k slots, not 8k; scaling this by the server's total connection count
    // would be wrong in the other direction.
    //
    // What it does have to track is how many connections *this* reactor can be
    // asked to serve, because that is what a broadcast costs. A flat 8192
    // looked generous until the box grew: past roughly that many connections
    // one broadcast fills whichever queue it lands on, and everything queued
    // after it is counted as queue_drops -- which reads as "the peers are not
    // draining" rather than "a constant was picked before anyone knew the
    // fan-out". Four frames of headroom per expected connection is enough for a
    // reply plus a close per broadcast, and the floor keeps small servers where
    // they were.
    static constexpr size_t k_min_pending_tasks = 8192;
    size_t pending_task_cap_ = k_min_pending_tasks;
    std::mutex              pending_mutex_;
    std::vector<queue_task> pending_tasks_;
    std::vector<queue_task> active_tasks_;

    // Deferred close notifications, one entry per closed connection. The four
    // fields used to be four parallel vectors "cleared together so their
    // indices always agree" -- which they did not, under the only condition
    // that can fail here: inserting four things can succeed three times. An
    // entry is now one object, so a partial notification is not representable.
    std::vector<closed_conn> pending_closes_;

    std::unordered_map<int, connection*>                      fd_index_;
    std::unordered_map<uint64_t, std::shared_ptr<connection>> conns_;

    std::vector<uint8_t> frame_scratch_;
};

// Defined after the class: GCC will not instantiate a member-function template
// from an in-class initializer. Inline so every translation unit that includes
// the header shares one definition.
inline constexpr sub_reactor::dispatch_table sub_reactor::k_task_dispatch = (
    sub_reactor::make_task_dispatch(std::make_index_sequence<
        std::variant_size_v<sub_reactor::queue_task>>{}));

// ==================== multi_reactor_server ====================
class multi_reactor_server {
 public:
    // Install handlers before start; never replace them while running. Handlers
    // run concurrently across workers and must not throw or destroy this
    // server.
    // Message payload/peer/reason references are borrowed for the call only.
    // send_to/close_connection are permitted. stop from a callback requests
    // shutdown only; the controlling thread must subsequently call stop to join
    // threads.
    // Close/state-false callbacks are deferred: they run on the worker thread
    // after the closing connection's stack frame has unwound. That deferral is
    // what makes stop() from a callback safe -- the callback is off the
    // connection's stack -- and it is NOT a licence to destroy the server: the
    // callback still runs on a thread this server owns, and destroying it there
    // leaves that thread joinable, which terminates the process. The last
    // external owner may only be released after the controlling thread's stop()
    // has returned; ~multi_reactor_server names the violation if it happens.
    using message_handler = std::function<void(uint64_t, const uint8_t*,
                                               size_t)>;
    using conn_state_handler =
        std::function<void(uint64_t, const std::string&, bool)>;
    using close_handler = std::function<void(uint64_t, const std::string&,
                                             const char*)>;

    struct config {
        std::string bind_addr        = "0.0.0.0";
        // 0 means "kernel, pick one" -- and then port() is the only way to
        // learn what was picked, so start() succeeding says nothing about
        // whether anyone can reach the server until port() is read.
        uint16_t    port            = 0;
        // Must be > 0 (start() refuses otherwise): Linux clamps listen()'s
        // backlog only from above, so 0 would pass straight through as a
        // zero-length accept queue -- connections refused under load, silently.
        int         backlog         = 1024;
        int         num_reactors     = 0;
        int         max_connections  = 10000;
        int         idle_timeout_ms   = 120000;
        int         tick_ms          = 500;
        // A frame must complete within this deadline once its first byte
        // arrives; 0 means 5 x idle_timeout_ms. Unlike idle_timeout_ms this
        // measures progress, so a peer that keeps dribbling a partial frame
        // cannot hold the connection forever. Must be >= 0 (start() refuses
        // negative).
        int         frame_assembly_timeout_ms = 0;
        size_t      rbuf_capacity    = MAX_FRAME_SIZE;
        // One maximum frame, not 256KB. The write buffer is allocated per
        // accepted connection, so it is most of the per-connection memory cost:
        // 324.7KB at 256KB, 128.8KB here, with no throughput change in the
        // ingest benchmark. The default is MAX_FRAME_SIZE rather than a rounded
        // number: send_to refuses a frame that does not fit, so a smaller
        // buffer makes a maximum-size reply impossible to write, on an idle
        // connection, forever. start() still accepts a smaller one on purpose
        // -- send_results relies on that to exercise the send limits.
        size_t      wbuf_capacity    = MAX_FRAME_SIZE;
        // Matches the buffer above rather than repeating 256KB: start() clamps
        // this down to wbuf_capacity anyway, so a larger default would only be
        // a number that reads as a promise it does not keep.
        size_t      max_pending_bytes = MAX_FRAME_SIZE;
        size_t      write_budget_bytes = 256 * 1024;
        // Grace period for a half-closed connection: after our FIN went out we
        // keep reading until the peer's FIN, but never longer than this. Bounds
        // the descriptor cost of a peer that ACKs the FIN and never answers it.
        // Must be > 0 (start() refuses otherwise): with no deadline a parked
        // close leaks the fd and its admission slot forever.
        int         close_linger_ms    = 5000;

        // Optional handler offload pool. 0 (default) keeps the message handler
        // inline on the reactor; N > 0 lets a handler copy its payload and call
        // offload() so slow work does not block the reactor. Tasks submitted
        // for one connection run serially on the pool.
        int         handler_threads = 0;
        // Bound on queued offload tasks; offload() returns false when full.
        size_t      handler_queue_limit = 8192;

        // Ceiling on reactors. Beyond a handful past the core count the only
        // thing extra workers buy is more cross-thread hand-off.
        static constexpr int k_max_reactors = 64;
        static constexpr int k_max_handler_threads = 64;

        // 0 means "five idle periods"; a positive value is an absolute
        // override. int64_t because 5 * idle_timeout_ms can exceed int.
        int64_t resolved_frame_assembly_timeout_ms() const noexcept {
            if (frame_assembly_timeout_ms > 0) {
                return static_cast<int64_t>(frame_assembly_timeout_ms);
            }
            return 5LL * static_cast<int64_t>(idle_timeout_ms);
        }

        // What this configuration actually resolves to, with "0 means decide
        // for me" resolved and the ceiling applied. Two places computing it
        // independently is how a number like this ends up disagreeing with
        // itself, which is what was happening: start() clamped to a literal 64
        // while everything that needed to know the count had to guess again.
        int resolved_reactor_count() const noexcept {
            int n = num_reactors > 0
                        ? num_reactors
                        : static_cast<int>(std::thread::hardware_concurrency());
            if (n <= 0) n = 1;
            if (n > k_max_reactors) n = k_max_reactors;
            return n;
        }
    };

    explicit multi_reactor_server(config cfg) noexcept : cfg_(std::move(cfg)) {}
    // Destroying from inside a callback cannot work, and it must not be quiet
    // about it. stop() below declines to join the calling thread -- a thread
    // cannot join itself -- so accept_thread_ is still joinable when this body
    // returns, and std::thread::~thread calls std::terminate(). That message,
    // "terminate called without an active exception", names neither this server
    // nor the reason; a reader who cannot tell what died learns nothing. Say it
    // here, then let the terminate happen: there is no correct recovery -- the
    // fix is for the last owner to be released after the controlling thread's
    // stop() has joined, which is what the contract above now asks for.
    ~multi_reactor_server() noexcept {
        if (callback_server_ == this) {
            std::fprintf(stderr,
                         "[fatal] multi_reactor_server destroyed from "
                         "inside its own callback: stop() cannot join the "
                         "calling thread, "
                         "so the accept/worker threads are still joinable and "
                         "std::thread::~thread is about to terminate. Call "
                         "stop() from the controlling thread, and release the "
                         "last owner only after it has returned.\n");
            std::fflush(stderr);
        }
        stop();
    }

    multi_reactor_server(const multi_reactor_server&) = delete;
    multi_reactor_server& operator=(const multi_reactor_server&) = delete;

    void set_message_handler(message_handler h) noexcept {
        msg_handler_ = std::move(h);
    }
    void set_conn_state_handler(conn_state_handler h) noexcept {
        state_handler_ = std::move(h);
    }
    // reason is borrowed and valid for the callback; includes shutdown
    // closures.
    void set_close_handler(close_handler h) noexcept {
        close_handler_ = std::move(h);
    }

    bool start() noexcept;
    void stop()  noexcept;
    // Stop accepting, ask every open connection to close gracefully, and wait
    // up to timeout_ms for them to disappear before the normal (abortive) stop.
    // Returns true if every connection drained; false if the deadline was hit
    // and the remainder were aborted. Call from the controlling thread, not a
    // callback.
    bool stop_gracefully(int timeout_ms) noexcept;
    void request_stop() noexcept { request_stop_internal(); }
#ifdef INDUSTRIAL_TCP_TESTING
    std::atomic<bool> fail_worker_for_test{false};
    std::atomic<bool> fail_accept_for_test{false};
#endif
    void wait()  noexcept;

    // ok means accepted (written straight to wbuf on the owner thread, or
    // queued otherwise), not delivered. Shutdown or disconnection can discard
    // work.
    send_result send_to(uint64_t conn_id, const void* data,
                        size_t len) noexcept;
    // True means the request reached the owning reactor's queue. False means it
    // did not -- the reactor refused it (its queue is at the cap) or there is
    // no owner to hand it to -- and the connection is still open. Either way it
    // is counted into close_requests_dropped: a close nobody asked for is bad,
    // but a close nobody was told about is how a connection outlives its own
    // owner.
    bool       close_connection(uint64_t conn_id) noexcept;

    // Runs fn on a handler-pool worker; tasks submitted for one connection run
    // in order. Returns false when no pool is configured/running or its queue
    // is full. A message handler can copy its payload and call this, then
    // return; the reactor is not blocked. fn runs off the reactor thread, so
    // send_to from it uses the queued path.
    bool offload(uint64_t conn_id, std::function<void()> fn) noexcept;

    size_t connection_count() const noexcept;
    std::vector<uint64_t> connection_ids() const;

    // Where the listening socket actually is. Equals config::port unless that
    // was 0, in which case this is the ephemeral port the kernel chose -- the
    // only place that number exists after bind(). 0 before start() has bound
    // and after stop() has closed: no socket, no port, and a stopped server
    // must not keep reporting one. Read it after start(), not before.
    uint16_t port() const noexcept;

    const server_stats& stats() const noexcept { return stats_; }

    // sub_reactor internal interface
    bool   on_connection_opened(uint64_t id,
                                std::shared_ptr<connection> c) noexcept;
    void   on_connection_closed(uint64_t id) noexcept;
    // running means: lifecycle reached running AND no component has failed.
    // A reactor/accept thread failure only raises failed_; it never rewrites
    // the lifecycle state, so start()'s success/failure verdict stays
    // unambiguous.
    bool   is_running() const noexcept {
        return life_.load(std::memory_order_acquire) == life::running &&
               !failed_.load(std::memory_order_acquire);
    }
    int    tick_ms() const noexcept { return cfg_.tick_ms; }
    int    idle_timeout_ms() const noexcept { return cfg_.idle_timeout_ms; }
    int    close_linger_ms() const noexcept { return cfg_.close_linger_ms; }
    size_t rbuf_capacity() const noexcept { return cfg_.rbuf_capacity; }
    size_t wbuf_capacity() const noexcept { return cfg_.wbuf_capacity; }
    server_stats&       stats_ref() noexcept { return stats_; }
    message_handler&    msg_handler_ref() noexcept { return msg_handler_; }
    conn_state_handler& state_handler_ref() noexcept {
        return state_handler_;
    }

 private:
    friend class sub_reactor;

    // Lifecycle states. Only the thread holding lifecycle_mutex_ moves the
    // state out of/into stopped; threads that merely observe a fatal error use
    // fail().
    enum class life : int {
        stopped = 0, starting = 1, running = 2, stopping = 3
    };

    // Single upward-only failure flag shared by the accept loop and all
    // reactors.
    // Independent of the lifecycle state so it can be raised concurrently.
    void fail() noexcept { failed_.store(true, std::memory_order_release); }
    // Step down from running and wake everyone; no-op unless running.
    void  request_stop_internal() noexcept {
        life expected = life::running;
        if (life_.compare_exchange_strong(expected, life::stopping,
                                          std::memory_order_acq_rel)) {
            wake_accept_loop();
            // The reactors are not woken here, so one of them may sit in
            // epoll_wait for up to tick_ms before it sees the state and leaves.
            // Waking them means touching reactors_ from here, and this runs on
            // three different threads: a caller's, the accept loop's, and a
            // callback's (sub_reactor::stop() routes a stop from inside a
            // callback through here). reactors_ is only mutated by stop(),
            // under lifecycle_mutex_ -- which cannot be taken here, because
            // that same callback path already holds it. So the choice is a lock
            // that deadlocks or a read that races a clear().
            //
            // Left as it is on purpose: stop() wakes every reactor itself and
            // joins them, so only the request_stop() + wait() pairing sees
            // this, and wait() polls at 200ms -- coarser than the tick_ms it
            // would save.
        }
    }
    void wake_accept_loop() noexcept;
    // Stop the accept thread and leave the reactors running. Idempotent; used
    // by stop_gracefully() before it closes the live connections, and by
    // stop().
    void stop_accepting() noexcept;
    // Optional handler offload pool; no-op when config::handler_threads == 0.
    bool start_handler_pool() noexcept;
    void stop_handler_pool() noexcept;
    void handler_worker() noexcept;

    bool        init_listen_socket() noexcept;
    // The one place the listening socket is closed, so that "no socket" and "no
    // port" cannot drift apart: a port() that outlives its socket reads like a
    // server that is still there.
    void        close_listen_socket() noexcept;
    void        accept_loop(std::promise<bool>& ready) noexcept;
    sub_reactor* pick_reactor() noexcept;

    config cfg_;
    int listen_fd_ = -1;
    int epoll_fd_  = -1;
    int wake_fd_   = -1;
    // Resolved from the bound socket, never from cfg_. The obvious alternative
    // -- writing it back into cfg_.port after bind() -- makes the next start()
    // after a stop() bind last run's throwaway number instead of asking for a
    // fresh one, which is a worse surprise than the one being fixed.
    std::atomic<uint16_t> bound_port_{0};

    std::atomic<life>     life_{life::stopped};
    std::atomic<bool>     failed_{false};
    // True while the accept thread may hand new connections to reactors.
    // Cleared before a graceful drain so no new connection arrives after the
    // snapshot; stop() clears it as well.
    std::atomic<bool>     accepting_{false};
    std::atomic<uint64_t> next_conn_id_{1};
    std::atomic<size_t>   next_reactor_{0};
    // Lifecycle calls are serialized; submissions hold their barrier only until
    // queued. stop releases it before joining so worker callbacks can finish.
    std::mutex lifecycle_mutex_;
    // Covers publication, wake writes, and fd teardown.
    std::mutex accept_fd_mutex_;
    inline static thread_local multi_reactor_server* callback_server_ = nullptr;
    struct callback_scope {
        multi_reactor_server* previous;
        explicit callback_scope(multi_reactor_server* server) noexcept
            : previous(callback_server_) { callback_server_ = server; }
        ~callback_scope() { callback_server_ = previous; }
    };
    // The non-owner send/close path used to serialize on one submission lock
    // and one connection-map lock per call. Shard both by connection id: a
    // sender contends only with senders for the same shard, and stop() takes
    // every shard once to drain them all. The shards are line-aligned so two
    // locks never share a cache line.
    static constexpr size_t k_conn_shards = 16;
    struct alignas(64) submission_shard {
        std::shared_mutex mutex;
    };
    struct alignas(64) conn_shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<uint64_t, std::shared_ptr<connection>> map;
    };
    static size_t conn_shard_for(uint64_t id) noexcept {
        return static_cast<size_t>(id) % k_conn_shards;
    }
    std::array<submission_shard, k_conn_shards> submission_shards_;
    std::array<conn_shard, k_conn_shards> conn_shards_;
    // Queued accepted sockets + live connections, owned by admission tickets.
    std::atomic<size_t> admitted_{0};

    std::vector<std::unique_ptr<sub_reactor>> reactors_;
    std::thread                              accept_thread_;

    std::vector<std::thread> handler_threads_;
    std::mutex               handler_mutex_;
    std::condition_variable  handler_cv_;
    std::unordered_map<uint64_t, std::deque<std::function<void()>>>
        handler_queues_;
    std::unordered_set<uint64_t> handler_active_;
    std::deque<uint64_t>         handler_ready_;
    std::atomic<bool>            handler_running_{false};
    std::atomic<size_t>          handler_queued_{0};

    server_stats      stats_;
    message_handler   msg_handler_;
    conn_state_handler state_handler_;
    close_handler     close_handler_;
};

// ====================================================================
//                         sub_reactor implementation
// ====================================================================

inline bool sub_reactor::start() noexcept {
    if (running_.exchange(true)) return true;

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        running_.store(false); return false;
    }

    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        ::close(epoll_fd_); epoll_fd_ = -1;
        running_.store(false); return false;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = wake_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
        ::close(wake_fd_); ::close(epoll_fd_);
        wake_fd_ = epoll_fd_ = -1; running_.store(false); return false;
    }

    // Computed here rather than in the constructor: picking reactor count and
    // connection ceiling are both decisions the server makes later, and this is
    // the first point at which both are final.
    const size_t share =
        static_cast<size_t>(server_->cfg_.max_connections) /
        static_cast<size_t>(server_->cfg_.resolved_reactor_count());
    pending_task_cap_ = std::max(k_min_pending_tasks, share * 4);
    try {
        pending_tasks_.reserve(pending_task_cap_);
        active_tasks_.reserve(pending_task_cap_);
        frame_scratch_.resize(MAX_FRAME_PAYLOAD);
    } catch (...) {
        ::close(wake_fd_); ::close(epoll_fd_);
        wake_fd_ = epoll_fd_ = -1; running_.store(false); return false;
    }

    try {
        thread_ = std::thread([this]() noexcept { loop(); });
    } catch (...) {
        ::close(wake_fd_); ::close(epoll_fd_);
        wake_fd_ = epoll_fd_ = -1; running_.store(false); return false;
    }
    return true;
}

inline void sub_reactor::stop() noexcept {
    if (server_->callback_server_ == server_) {
        server_->request_stop(); return;
    }
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        running_.store(false);
        wakeup();
    }
    if (thread_.joinable()) thread_.join();
    cleanup();
    std::lock_guard<std::mutex> lk(pending_mutex_);
    if (wake_fd_  >= 0) {
        ::close(wake_fd_);  wake_fd_  = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_); epoll_fd_ = -1;
    }
}

inline void sub_reactor::cleanup() noexcept {
    multi_reactor_server::callback_scope callbacks(server_);
    // Discard waiting tasks. A new_conn_task's fd_guard closes an unclaimed
    // socket; a send_task's frame vector is freed with the task.
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        server_->stats_ref().pending_tasks_depth.fetch_sub(
            pending_tasks_.size(), std::memory_order_relaxed);
        pending_tasks_.clear();
    }

    while (!conns_.empty()) {
        // Shutdown cannot wait out anyone's half-close -- the reactor is
        // leaving and there is no thread left to deliver a parked connection's
        // last bytes. Abort explicitly, and account the undelivered bytes as
        // drops rather than pretending they went out.
        std::shared_ptr<connection> c = conns_.begin()->second;
        connection* raw = c.get();
        c.reset();
        close_conn(raw, "server shutdown", false);
    }
    flush_close_callbacks();
    active_tasks_.clear();
    conns_.clear();
    fd_index_.clear();
}

inline void sub_reactor::wakeup() noexcept {
    if (wake_fd_ < 0) return;
    uint64_t v = 1;
    ssize_t r = ::write(wake_fd_, &v, sizeof(v));
    (void)r;
}

// eventfd clears its accumulated count in one read, so there is nothing to
// loop over.
inline void sub_reactor::drain_wakeup() noexcept {
    uint64_t v = 0;
    ssize_t r = ::read(wake_fd_, &v, sizeof(v));
    (void)r;
}

inline void sub_reactor::process_pending() noexcept {
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        if (pending_tasks_.empty()) return;
        active_tasks_.swap(pending_tasks_);
        server_->stats_ref().pending_tasks_depth.fetch_sub(
            active_tasks_.size(), std::memory_order_relaxed);
    }
    for (auto& t : active_tasks_) {
        if (!running_.load() || !server_->is_running()) break;
        // One indirect call through the table, indexed by the alternative.
        k_task_dispatch[t.index()](this, t);
        flush_close_callbacks();
    }
    active_tasks_.clear();
    flush_close_callbacks();
}

inline bool sub_reactor::enqueue(queue_task task) noexcept {
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        if (!running_.load() || !server_->is_running() ||
            pending_tasks_.size() >= pending_task_cap_) {
            server_->stats_ref().queue_drops.fetch_add(
                1, std::memory_order_relaxed);
            return false;
        }
        // Both batch vectors reserve this limit before the worker starts.
        pending_tasks_.emplace_back(std::move(task));
        server_->stats_ref().pending_tasks_total.fetch_add(
            1, std::memory_order_relaxed);
        server_->stats_ref().pending_tasks_depth.fetch_add(
            1, std::memory_order_relaxed);
        // Serialize notification with stop's descriptor cleanup. No producer
        // touches wake_fd_ after releasing this lock.
        wakeup();
    }
    return true;
}

inline bool sub_reactor::submit_new_connection(
        fd_guard fd, std::string peer, uint64_t id,
        std::shared_ptr<admission_ticket> admission) noexcept {
    try {
        auto task = std::make_unique<new_conn_task>();
        task->fd = std::move(fd);
        task->peer = std::move(peer);
        task->id = id;
        task->admission = std::move(admission);
        return enqueue(queue_task{std::move(task)});
    } catch (...) {
        // The local fd_guard closes the socket; nothing was offered to the
        // queue, so the accept loop counts this as rejected.
        return false;
    }
}

inline void sub_reactor::handle_task(
        std::unique_ptr<new_conn_task>& task) noexcept {
    new_conn_task& t = *task;
    try {
        auto c = std::make_shared<connection>();
        c->id = t.id;
        c->peer = std::move(t.peer);
        c->owner = this;
        c->admission = std::move(t.admission);
        c->last_recv_ms = now_ms();
        c->rbuf.init(server_->rbuf_capacity());
        c->wbuf.init(server_->wbuf_capacity());

        struct epoll_event ev{};
        ev.events = EPOLLIN; ev.data.fd = t.fd.fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, t.fd.fd, &ev) < 0) {
            server_->stats_ref().epoll_ctl_errors.fetch_add(
                1, std::memory_order_relaxed);
            server_->stats_ref().rejected.fetch_add(
                1, std::memory_order_relaxed);
            return;  // The task's fd_guard closes the descriptor.
        }
        c->fd = t.fd.release();

        try {
            fd_index_[c->fd] = c.get();
            conns_[t.id] = c;
        } catch (...) {
            // Roll back through the same path every other failure uses, so the
            // epoll registration does not outlive the map entry and the fd
            // number is not silently re-attributed when it is recycled -- and
            // the map entry does not outlive the descriptor either:
            // unregister_conn() alone left a ghost behind.
            discard_rejected_conn(c.get());
            server_->stats_ref().rejected.fetch_add(
                1, std::memory_order_relaxed);
            return;
        }
        if (!server_->on_connection_opened(t.id, c)) {
            discard_rejected_conn(c.get());
            server_->stats_ref().rejected.fetch_add(
                1, std::memory_order_relaxed);
            return;
        }
        // Counted only once the socket is fully owned by both maps: an accepted
        // client must not inflate accepted when the reactor then rejects it,
        // otherwise accepted - closed stops matching reality.
        server_->stats_ref().accepted.fetch_add(
            1, std::memory_order_relaxed);

        // Nothing after this line may count rejected. Every earlier failure
        // returns while the socket is still unregistered or rolls back
        // through discard_rejected_conn(); from here the connection is in both
        // maps, owns its epoll registration, answers connection_count(), can
        // be sent to, and will be counted in closed when it goes. So if the
        // state callback throws now, incrementing rejected too would make
        // the two counters contradict each other about one live connection
        // -- and accepted is the one telling the truth, which leaves
        // rejected as the counter an operator reads to look for clients
        // being turned away. (Nothing counts the throw either: server_stats
        // has no slot for "the application's callback threw" that would not
        // read as a claim about this server, and the caller's own handler is
        // where the exception belongs -- swallowing it here is exactly what
        // lets whoever wrote it find out.)
        auto& sh = server_->state_handler_ref();
        if (sh) {
            try {
                sh(t.id, c->peer, true);
            } catch (...) {
                // See above: nothing to undo, nothing to count.
            }
        }
    } catch (...) {
        // Before registration, the task's fd_guard closes the socket.
        server_->stats_ref().rejected.fetch_add(
            1, std::memory_order_relaxed);
    }
}

inline bool sub_reactor::submit_send(uint64_t id,
                                   std::vector<uint8_t> frame) noexcept {
    return enqueue(queue_task{send_task{id, std::move(frame)}});
}

inline void sub_reactor::handle_task(send_task& task) noexcept {
    const uint64_t id = task.id;
    std::vector<uint8_t>& f = task.frame;
    auto it = conns_.find(id);
    if (it == conns_.end()) return;
    connection* c = it->second.get();
    // A parked close still accepts the reply queued before it: submit_close
    // only marks the connection and then returns to this batch, so this
    // task runs first. Anything arriving after that point is refused here
    // -- the FIN is already reserved for what we owe the peer.
    if (c->is_closed.load(std::memory_order_acquire) ||
        c->closing.load(std::memory_order_acquire)) {
        c->pending_bytes.fetch_sub(f.size(), std::memory_order_relaxed);
        // One refusal, two questions, two counters: how many replies never went
        // out (queue_drops) and how much data the peer never received
        // (bytes_dropped). Recording whichever one this branch happened to
        // reach made the two incomparable across a close -- the same event
        // would show up as a frame drop or a byte drop depending on whether the
        // teardown had finished yet.
        server_->stats_ref().bytes_dropped.fetch_add(
            f.size(), std::memory_order_relaxed);
        server_->stats_ref().queue_drops.fetch_add(
            1, std::memory_order_relaxed);
        return;
    }

    // Whole frames are written atomically: short capacity drops the frame
    // rather than splitting it and corrupting the stream.
    //
    // Reachable in principle, not in any configuration this file accepts:
    // start() clamps max_pending_bytes down to wbuf_capacity, a frame's
    // reservation stays in pending_bytes until it is written, and wbuf's
    // contents are a subset of what pending_bytes covers -- so wbuf.writable()
    // >= f.size() holds here by construction. Measured under 8 concurrent
    // producers, 38MB actually sent, to the point of 35369 refusals elsewhere:
    // this branch fired 0 times.
    //
    // Kept rather than asserted away, because the alternative to refusing is a
    // partial frame in wbuf, which corrupts the stream. It records both
    // counters now, as the close branch above does: a refusal is one event that
    // operations may want to read either way, and recording whichever one this
    // branch reached is what made them incomparable.
    if (c->wbuf.writable() < f.size()) {
        c->pending_bytes.fetch_sub(f.size(), std::memory_order_relaxed);
        server_->stats_ref().bytes_dropped.fetch_add(
            f.size(), std::memory_order_relaxed);
        server_->stats_ref().queue_drops.fetch_add(
            1, std::memory_order_relaxed);
        return;
    }
    c->wbuf.write(f.data(), f.size());
    // No release here: this is step 2 above, the byte count moving from
    // "reserved by this task" to "reserved in wbuf". The drop branch just above
    // does subtract, and the drain in handle_write subtracts again later -- so
    // "unchanged" is a statement about this path, not about the counter's
    // behaviour anywhere else.
    // No "was empty" test: arm_write is an idempotent move toward a target
    // state, so arming twice has no effect -- while wbuf.empty() cannot see the
    // part of pending_bytes that is reserved but not yet written, and deciding
    // whether to arm from it loses a real write interest when it races
    // disarm_write, stranding frames already sitting in wbuf. frames_sent is
    // not counted here either: a frame is counted as it leaves wbuf for the
    // kernel (see drain_wbuf_to_socket), the same point as bytes_sent.
    arm_write(c);
}

// The owner-thread half of send_to(). A message handler already runs on the
// reactor that owns the connection, so the frame can go into wbuf without the
// global submission/connection locks, the task queue, its eventfd wakeup, or
// the frame vector the queued path allocates. The queue is still the only path
// for application threads and for connections owned by another reactor.
inline bool sub_reactor::try_send_local(multi_reactor_server* expected,
                                     uint64_t id, const void* data, size_t len,
                                     send_result* out) noexcept {
    if (server_ != expected) return false;   // another server owns this thread
    auto it = conns_.find(id);
    if (it == conns_.end()) return false;   // another reactor owns it

    connection* c = it->second.get();
    if (c->is_closed.load(std::memory_order_acquire) ||
        c->closing.load(std::memory_order_acquire)) {
        *out = send_result::closed;
        return true;
    }
    // A close is already queued ahead of this send: take the queued path so the
    // frame is ordered after it and dropped, exactly as before.
    if (c->close_requested.load(std::memory_order_acquire)) return false;

    const size_t frame_size = FRAME_HEADER_SIZE + len;
    // No room now: take the queued path, which reserves the frame and lets the
    // task decide later. That is what preserves the existing backpressure
    // accounting for a full wbuf.
    if (c->wbuf.writable() < frame_size) return false;

    size_t old = c->pending_bytes.load(std::memory_order_relaxed);
    for (;;) {
        if (old > server_->cfg_.max_pending_bytes ||
            frame_size > server_->cfg_.max_pending_bytes - old) {
            server_->stats_ref().queue_drops.fetch_add(
                1, std::memory_order_relaxed);
            *out = send_result::overloaded;
            return true;
        }
        if (c->pending_bytes.compare_exchange_weak(
                old, old + frame_size,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) break;
    }

    uint8_t hdr[FRAME_HEADER_SIZE];
    write_frame_header(hdr, frame_type::DATA, len);
    const size_t hdr_wrote = c->wbuf.write(hdr, sizeof hdr);
    const size_t body_wrote =
        len ? c->wbuf.write(static_cast<const uint8_t*>(data), len) : 0;
    if (hdr_wrote != sizeof hdr || (len != 0 && body_wrote != len)) {
        // The capacity check above makes this unreachable; if it ever happens,
        // do not leave a partial frame in the stream.
        c->pending_bytes.fetch_sub(frame_size, std::memory_order_relaxed);
        close_conn(c, "wbuf short write", false);
        *out = send_result::overloaded;
        return true;
    }
    // frames_sent is counted as the frame leaves wbuf for the kernel, the same
    // point as the queued path and bytes_sent.
    arm_write(c);
    *out = send_result::ok;
    return true;
}

inline bool sub_reactor::submit_close(uint64_t id) noexcept {
    return enqueue(queue_task{close_task{id}});
}

inline void sub_reactor::handle_task(close_task& task) noexcept {
    const uint64_t id = task.id;
    auto it = conns_.find(id);
    if (it != conns_.end()) {
        // Copy out and reset the map handle before closing: the deferred close
        // callbacks must not run while this frame holds the conn.
        std::shared_ptr<connection> c = it->second;
        connection* raw = c.get();
        c.reset();
        // Default (graceful): this is an application-level "we are done", so
        // the reply queued just before it must still reach the peer -- and so
        // must answers to requests the peer already sent but this reactor has
        // not read yet.
        drain_input_then_close(raw);
    }
    flush_close_callbacks();
}

inline void sub_reactor::handle_task(finish_peer_closed_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    // Copy the pointer, then drop this frame's reference before the resumed
    // check: it may tear the connection down.
    std::shared_ptr<connection> keep = it->second;
    connection* raw = keep.get();
    keep.reset();
    finish_peer_closed(raw);
}

inline void sub_reactor::handle_task(drain_input_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    std::shared_ptr<connection> keep = it->second;
    connection* raw = keep.get();
    keep.reset();
    drain_input_then_close(raw);
}

inline void sub_reactor::handle_task(park_close_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    // Release the map's reference only after copying the pointer, so this frame
    // holds nothing while park_close may close the connection.
    std::shared_ptr<connection> keep = it->second;
    connection* raw = keep.get();
    keep.reset();
    park_close(raw, "requested");
}

inline void sub_reactor::arm_write(connection* c) noexcept {
    // Nothing asked for can ever leave a dead write side, and asking for it
    // each time bytes arrive would spin this reactor on level-triggered
    // EPOLLOUT for the rest of the connection's life.
    if (c->write_armed || c->fd < 0) return;
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT; ev.data.fd = c->fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, c->fd, &ev) == 0) {
        c->write_armed = true;
        return;
    }
    if (errno == ENOENT) {
        // Registered set out of sync (fd was re-added elsewhere): restore it
        // with ADD and adopt the write interest in the same call.
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, c->fd, &ev) == 0) {
            c->write_armed = true;
            return;
        }
    }
    server_->stats_ref().epoll_ctl_errors.fetch_add(
        1, std::memory_order_relaxed);
    close_conn(c, "epoll write registration failed", false);
}

inline void sub_reactor::disarm_write(connection* c) noexcept {
    // Target state, not a toggle: the flag says what the EPOLLOUT bit should
    // be, so the transition is skipped when the bit is already right and issued
    // when it is not. The two are not the same question -- after our FIN the
    // write interest is gone while write_armed is still true, and skipping the
    // MOD there would leave a half-closed connection spinning on a
    // level-triggered event it can never consume.
    if (!c->write_armed || c->fd < 0) return;
    // No "still has bytes to flush" guard belongs here: dropping the interest
    // while wbuf holds bytes is exactly what strands them, and both callers
    // (handle_write, after the drain emptied wbuf, and the fin_sent path) have
    // already established there is nothing left to write. Leaving it to the
    // caller is what keeps this function a plain state transition.
    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = c->fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, c->fd, &ev) < 0 &&
        errno != ENOENT) {
        server_->stats_ref().epoll_ctl_errors.fetch_add(
            1, std::memory_order_relaxed);
        close_conn(c, "epoll read registration failed", false);
        return;
    }
    c->write_armed = false;
}

inline drain_outcome sub_reactor::drain_wbuf_to_socket(connection* c) noexcept {
    size_t budget = server_->cfg_.write_budget_bytes;
    while (!c->wbuf.empty()) {
        if (!budget) return drain_outcome::stalled;
        auto s = c->wbuf.spans();
        // Send both wrapped ring segments in one syscall, within the fair-share
        // byte budget. A larger configured budget trades latency for
        // throughput.
        iovec spans[2]{};
        spans[0].iov_base = const_cast<uint8_t*>(s.p1);
        spans[0].iov_len = std::min(s.n1, budget);
        spans[1].iov_base = const_cast<uint8_t*>(s.p2);
        spans[1].iov_len = std::min(s.n2, budget - spans[0].iov_len);
        msghdr msg{};
        msg.msg_iov = spans;
        msg.msg_iovlen = spans[1].iov_len ? 2 : 1;
        ssize_t k = ::sendmsg(c->fd, &msg, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return drain_outcome::stalled;
            // EPIPE: this socket will never take another byte. Returning is not
            // the same thing as EAGAIN above, and the callers are the ones that
            // have to know the difference -- see handle_write and park_close.
            if (errno == EPIPE) {
                server_->stats_ref().epipe_writes.fetch_add(
                    1, std::memory_order_relaxed);
                return drain_outcome::dead;
            }
            return drain_outcome::error;
        }
        if (k == 0) return drain_outcome::error;
        // frame accounting runs before consume(): the readable region still
        // holds the bytes that just left, so a frame starting inside this chunk
        // still has its header there to read. A frame is counted when its last
        // byte reaches the kernel -- the same boundary bytes_sent counts at,
        // which is what keeps the two reconcilable. Counting on queueing
        // instead meant an abort could drop frames the counter had already
        // claimed, with nothing left to subtract them from.
        {
            size_t off  = 0;   // where the current frame starts, from the head
            size_t left = static_cast<size_t>(k);
            while (left > 0) {
                if (c->send_frame_remain == 0) {
                    // Whole frames only ever enter wbuf, so a boundary inside
                    // the readable region always has a full header behind it.
                    uint8_t hdr[FRAME_HEADER_SIZE];
                    c->wbuf.peek_at(off, hdr, FRAME_HEADER_SIZE);
                    const uint32_t flen = (static_cast<uint32_t>(hdr[4]) << 24)
                                        | (static_cast<uint32_t>(hdr[5]) << 16)
                                        | (static_cast<uint32_t>(hdr[6]) <<  8)
                                        |  static_cast<uint32_t>(hdr[7]);
                    c->send_frame_remain = FRAME_HEADER_SIZE + flen;
                }
                const size_t take = std::min(left, c->send_frame_remain);
                c->send_frame_remain -= take;
                left -= take;
                off += take;
                if (c->send_frame_remain == 0) ++frames_sent_local_;
            }
        }
        c->wbuf.consume(static_cast<size_t>(k));
        budget -= static_cast<size_t>(k);
        c->pending_bytes.fetch_sub(
            static_cast<size_t>(k), std::memory_order_relaxed);
        bytes_sent_local_ += static_cast<uint64_t>(k);
    }
    return drain_outcome::drained;
}

// Reads and parsing interleave under a per-call budget: no single client can
// own this reactor, and a full rbuf that still holds whole frames is never
// mistaken for a peer that has gone quiet.
inline int sub_reactor::handle_read(connection* c) noexcept {
    unschedule_read(c);
    constexpr int k_read_budget  = 64 * 1024;
    constexpr int k_frame_budget = 256;
    int bytes_left = k_read_budget, frames_left = k_frame_budget;
    // Frames this call handed to the handler. Every exit reports it, because
    // "the peer asked nothing more" and "the peer asked, and answers for it are
    // now queued behind whichever task called us" are not distinguishable from
    // outside -- and only the second one has something to wait for.
    auto dispatched = [&frames_left] { return k_frame_budget - frames_left; };

    for (;;) {
        if (!server_->is_running()) return dispatched();
        // Parse whatever is already a complete frame
        parse_frames(c, frames_left);
        if (c->is_closed.load(std::memory_order_acquire)) return dispatched();
        if (frames_left <= 0 || bytes_left <= 0) {
            schedule_read(c);
            return dispatched();
        }

        if (c->rbuf.writable() == 0) {
            // Still full after parsing: the partial frame can never fit,
            // close it.
            close_conn(c, "rbuf full with incomplete frame", false);
            return dispatched();
        }

        // writable() was checked above and tail < cap_ always, so write_span()
        // offers at least one byte. Near the end of the ring it offers only the
        // contiguous piece, down to one byte; the next recv() starts at the
        // wrapped tail. A read is never split across the wrap.
        size_t want = std::min(c->rbuf.write_span(),
                               static_cast<size_t>(bytes_left));
        assert(want > 0);
        ssize_t n = ::recv(c->fd, c->rbuf.write_ptr(), want, 0);
        if (n > 0) {
            c->rbuf.commit(static_cast<size_t>(n));
            bytes_left -= static_cast<int>(n);
            // Only ever read by drain_input_then_close(), which zeroes it
            // first: it has to know how much of what this call served was
            // input that arrived during the call, because a frame that rides
            // in on the same read as the backlog must be charged to the close
            // budget too.
            c->drain_read_bytes += static_cast<size_t>(n);
            // Stamped here, on arrival, not when a frame completes: a frame
            // that took longer than idle_timeout_ms to arrive would otherwise
            // be closed while it was still arriving -- the peer had never gone
            // quiet and bytes_recv was still climbing. "Idle" means no bytes,
            // and this is where bytes appear.
            c->last_recv_ms = now_ms();
            bytes_recv_local_ += static_cast<uint64_t>(n);
        } else if (n == 0) {
            // peer's FIN. If we still owe it a reply, closing here would turn
            // the last answer into an RST -- the client asked, we processed,
            // and it never sees the result. Park the close instead where we
            // can: after our own half-close, or (for an application close) with
            // the FIN to go out once wbuf drains.
            server_->stats_ref().eof_reads.fetch_add(
                1, std::memory_order_relaxed);
            c->peer_closed = true;
            // No re-parse here, and none is missing. Two things hold at this
            // point: parse_frames() at the top of the loop has just consumed
            // everything rbuf could yield (otherwise frames_left would have hit
            // 0 and the loop would have rescheduled and returned before the
            // read), and recv() returning 0 adds no bytes. So rbuf is
            // byte-for-byte what the parse above already saw, and parsing it
            // again can only find nothing -- which is why the old call here
            // looked like a second chance and was not one.
            //
            // No "budget spent, frames still buffered" case can exist here, for
            // the same reason. (Measured: eof_reads stays at 1 across bursts of
            // 1..2000 frames, which is what makes it usable as the busy-poll
            // probe of P1-1b.)
            assert(frames_left > 0);
            finish_peer_closed(c);
            return dispatched();
        } else {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Same reasoning as the n == 0 branch: recv() added nothing, so
                // there is nothing new to parse. The parse that used to sit
                // here could not have found a frame the one at the top of the
                // loop had not already taken.
                return dispatched();
            }
            close_conn(c, "recv error", false); return dispatched();
        }
    }
}

inline void sub_reactor::handle_write(connection* c) noexcept {
    // Past our FIN there is nothing left to write, and the EPOLLOUT bit is kept
    // clear, so an event arriving here is stale: just make sure it is off.
    if (c->fin_sent) {
        disarm_write(c); return;
    }
    const drain_outcome r = drain_wbuf_to_socket(c);
    if (r == drain_outcome::error) {
        close_conn(c, "send error", false); return;
    }
    // Nothing more will ever leave this socket, so there is nothing for the
    // rest of this function to wait for: no FIN can be negotiated (it requires
    // an empty wbuf first) and no later EPOLLOUT can deliver what is queued.
    // Ending it here rather than waiting is also what keeps this from becoming
    // the write-side echo of the busy-poll the read side was cured of -- a
    // socket with send-buffer space and a broken peer reports EPOLLOUT on every
    // single epoll_wait, and leaving the interest armed would have this reactor
    // re-run a failing sendmsg until some outside deadline noticed.
    if (r == drain_outcome::dead) {
        close_conn(c, "write side dead", false);
        return;
    }
    // fair-share budget or EAGAIN: EPOLLOUT refires
    if (!c->wbuf.empty()) return;

    // A parked peer-close finishes here, deterministically, instead of waiting
    // for the next spurious EPOLLIN to notice the drain completed.
    if (c->peer_closed) {
        finish_peer_closed(c);
        return;
    }

    // wbuf is empty, so anything owed to the peer has reached the kernel:
    // this is the only place a FIN may be sent. A non-closing connection just
    // drops its write interest here; a parked close turns into a half-close.
    // Why this is not guarded by pending_bytes, even though it sends a FIN.
    //
    // "wbuf is empty" is not the same statement as "nothing is owed" -- a frame
    // admitted by submit_send is counted in pending_bytes from the moment it is
    // queued until it lands in wbuf, and this branch is exactly the one that
    // reads wbuf without consulting that difference. So asking here whether
    // anything is still reserved is the obvious question, and the answer for
    // why it is not asked is an ordering invariant, which has to be written
    // down or the next reader re-derives it and lands somewhere else:
    //
    // reply tasks only reach the queue from three places -- a handler (running
    // in the read continuation or the event phase, both ahead of
    // process_pending()), another task, and a send_to() from whatever thread
    // the application used. The reactor sets `closing` from exactly three
    // places too: a task (submit_close / drain_input_then_close's round),
    // do_tick()'s sweeps, and park_close() called from this very branch. So:
    //   - against a task: every reply enqueued before it is ahead of it in the
    //     same FIFO batch, and process_pending() drains the batch in order, so
    //     the replies have landed by the time closing goes true;
    //   - against do_tick(): its sweeps run after process_pending() in the same
    //     iteration, so nothing a handler produced can still be pending;
    //   - the rest of the money is made elsewhere: finish_peer_closed() above
    //     is reached through the same guard N1 added, and handle_write is
    //     reached only through conns_, which is where closing comes from
    //     anyway.
    //
    // What remains is a submission that arrives between the queue drain and a
    // sweep, from another task or another thread. That one really can be FIN'd
    // over -- and it is the documented meaning of ok ("queued, not delivered;
    // shutdown or disconnection can discard work"), not a second meaning
    // layered on top of it: submit_send's task refuses it and accounts it in
    // queue_drops and bytes_dropped, so a reply lost this way is countable
    // rather than silent.
    // Adding a guard here would move that accounting nowhere and would make
    // every EPOLLOUT on a parked socket wait another iteration to ask again.
    disarm_write(c);
    if (c->closing.load(std::memory_order_acquire) &&
        !try_begin_write_shutdown(c)) {
        // shutdown() failed on a socket that was writable a moment ago: the
        // only way left to end this connection is a reset, which is at least
        // honest.
        teardown_closed(c, "shutdown failed", false);
    }
}

// Sends FIN and records it. Reads stay armed: the connection is now waiting for
// the peer's own FIN (or close_linger_ms, enforced by do_tick).
inline bool sub_reactor::try_begin_write_shutdown(connection* c) noexcept {
    if (c->fin_sent || c->fd < 0) return c->fin_sent;
    // Never FIN over bytes still in wbuf: they would be lost, and the peer
    // would see a clean end to a truncated stream.
    if (!c->wbuf.empty()) return false;
    if (::shutdown(c->fd, SHUT_WR) < 0) return false;
    c->fin_sent = true;
    c->fin_sent_at_ms = now_ms();
    return true;
}

// The peer has finished sending (recv returned 0). handle_read reaches this
// only once no complete frame is left unprocessed -- but "every request has
// run its handler" is NOT the same as "every reply is in wbuf": submit_send
// queues a task, so the answers produced by this very batch are still behind
// us in the queue, reserved in pending_bytes but not yet written. Tearing down
// here sends an orderly FIN over replies nobody ever sees (case E: 0 of 229432
// bytes).
//
// What happens next depends on what is still owed, in the order that keeps the
// FIN behind every byte:
//   1. wbuf holds bytes -- park, and drop EPOLLIN while parked: a FIN makes
//      the socket readable for the rest of its life, so with level-triggered
//      read interest still armed every epoll_wait returns at once and the
//      reactor busy-polls for the whole drain (measured: +35k FIN wakes in
//      250ms). handle_write resumes this once wbuf empties. A park that
//      cannot be registered is not a park -- the branch below ends the
//      connection instead, because the only thing a half-registered park can
//      do is spin.
//   2. wbuf empty but pending_bytes > 0 -- replies are queued behind this
//      batch. Defer exactly one queue round (deferred_round = peer_closed) so
//      they land in wbuf first; FIFO runs every task queued before this one
//      first. At most once: a producer racing the close must not hold the
//      connection open forever.
//   3. nothing outstanding  -- FIN (if we are closing) and tear down now.
//
// This is also the last chance to end the connection. Until it ran, an EOF with
// nothing else outstanding left the socket registered and idle: the read event
// had already fired, so nothing was going to fire again, and the connection sat
// in conns_ until do_tick's linger deadline reached it -- which aborts,
// because a deadline is all it can be sure of. Every half-close initiated by
// the peer paid for that with an RST it never earned (P0-5).
inline void sub_reactor::finish_peer_closed(connection* c) noexcept {
    if (c->is_closed.load(std::memory_order_acquire)) return;
    if (!c->wbuf.empty()) {
        // Parked with the write side still productive. Read interest is dropped
        // for the rest of the drain (see the header comment): the only thing a
        // read could deliver is the FIN we have already seen.
        if (c->fd >= 0) {
            struct epoll_event wev{};
            wev.events  = EPOLLOUT;
            wev.data.fd = c->fd;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, c->fd, &wev) == 0) {
                c->write_armed = true;
                return;                    // parked: handle_write resumes it
            }
            if (errno == ENOENT) {
                // Registered set out of sync, the same way arm_write can
                // find it. Recovering with ADD is not optional here: ENOENT
                // means the fd is in no interest list at all, so without this
                // the socket goes silent and the drain never resumes -- a peer
                // that comes back and reads gets what was already in flight
                // and then hangs (measured: 3MB of 16MB, then nothing).
                if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, c->fd, &wev) == 0) {
                    c->write_armed = true;
                    return;
                }
            }
            // any other failure: the interest list cannot be made to match what
            // this connection needs, and EPOLLIN is still armed on a socket the
            // peer has already FIN'd -- level-triggered, that is a read event
            // on every single epoll_wait for as long as the drain takes, and
            // every one of them re-runs recv() -> 0 -> this function -> the
            // same failing call (measured: ~170k/s, one whole core, up to
            // idle_timeout_ms + close_linger_ms, then a forced abort that drops
            // everything still owed). Same verdict arm_write and disarm_write
            // already reach for the same syscall: end it, rather than let a
            // connection that cannot be maintained be maintained anyway.
            server_->stats_ref().epoll_ctl_errors.fetch_add(
                1, std::memory_order_relaxed);
            close_conn(c, "epoll park registration failed", false);
            return;
        }
        // fd < 0 means the descriptor is already detached, and the only place
        // that detaches it is teardown_closed() -- which sets is_closed first,
        // so the guard at the top of this function has already returned.
        // Reaching here would mean a second detach path exists; the guard above
        // is kept so that adding one cannot turn into a blind epoll_ctl on fd
        // -1.
        return;                    // parked close finishes in handle_write
    }
    if (!c->closing.load(std::memory_order_acquire) &&
        c->deferred_round == connection::deferred_round_kind::none &&
        c->pending_bytes.load(std::memory_order_relaxed) > 0) {
        c->deferred_round = connection::deferred_round_kind::peer_closed;
        const uint64_t id = c->id;
        if (enqueue(queue_task{finish_peer_closed_task{id}})) return;
        // The hand-off failed (queue full, or the reactor is going down): a
        // late FIN beats an answer nobody can wait for any longer.
    }
    if (c->closing.load(std::memory_order_acquire) && !c->fin_sent) {
        // Still closing on our side and the peer declared itself done too: the
        // only thing left is the FIN. Anything a handler queued was drained
        // above or is still passing through the socket buffer, ahead of it.
        if (!try_begin_write_shutdown(c)) {
            teardown_closed(c, "peer closed", false); return;
        }
    }
    teardown_closed(c, "peer closed", true);
}

inline void sub_reactor::parse_frames(connection* c,
                                      int& frames_left) noexcept {
    uint8_t hdr[FRAME_HEADER_SIZE];
    while (server_->is_running() && frames_left > 0 &&
           c->rbuf.readable() >= FRAME_HEADER_SIZE) {
        c->rbuf.peek(hdr, FRAME_HEADER_SIZE);
        uint16_t magic = (static_cast<uint16_t>(hdr[0]) << 8) | hdr[1];
        if (magic != FRAME_MAGIC) {
            server_->stats_ref().parse_errors.fetch_add(
                1, std::memory_order_relaxed);
            close_conn(c, "bad magic", false); return;
        }
        frame_type type = static_cast<frame_type>(
            (static_cast<uint16_t>(hdr[2]) << 8) | hdr[3]);
        uint32_t len = (static_cast<uint32_t>(hdr[4]) << 24)
                     | (static_cast<uint32_t>(hdr[5]) << 16)
                     | (static_cast<uint32_t>(hdr[6]) <<  8)
                     |  static_cast<uint32_t>(hdr[7]);
        if (len > MAX_FRAME_PAYLOAD) {
            server_->stats_ref().parse_errors.fetch_add(
                1, std::memory_order_relaxed);
            close_conn(c, "frame too large", false); return;
        }
        // Partial frame, wait for more. Start the assembly deadline on the
        // first byte of this frame; later bytes do not refresh it, or a peer
        // could hold the connection forever by dribbling.
        if (c->rbuf.readable() < FRAME_HEADER_SIZE + len) {
            if (c->frame_started_at_ms == 0) c->frame_started_at_ms = now_ms();
            return;
        }

        c->rbuf.consume(FRAME_HEADER_SIZE);
        // This frame is complete; the next partial one starts a new deadline.
        c->frame_started_at_ms = 0;
        // No per-frame clock read here. last_recv_ms is stamped once per recv()
        // (handle_read), which is when bytes actually arrive; re-stamping it
        // while parsing only refreshes the idle deadline with the age of data
        // that already arrived. It also cost 18% of this reactor's CPU:
        // clock_gettime showed up as a fifth of parse_frames in perf, because
        // at 20M frames/s it is one call per 64 bytes.
        ++frames_recv_local_;
        --frames_left;

        if (type == frame_type::PING) {
            const auto& pong = pong_frame();
            size_t pending = c->pending_bytes.load(std::memory_order_relaxed);
            bool reserved = false;
            while (pending <= server_->cfg_.max_pending_bytes &&
                   pong.size() <= server_->cfg_.max_pending_bytes - pending) {
                if (c->pending_bytes.compare_exchange_weak(
                        pending, pending + pong.size(),
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    reserved = true;
                    break;
                }
            }
            // The whole-frame rule applies here too: a PONG that cannot go out
            // means the peer has no receive capacity left, so force the close.
            // The two causes are reported separately because their remedies
            // differ, and the wbuf half is unreachable by the invariant above
            // -- all that can really happen is pending_bytes hitting
            // max_pending_bytes, and calling that "wbuf full" sends whoever
            // reads the close callback the wrong way.
            if (!reserved) {
                close_conn(c, "pending bytes at limit on pong", false); return;
            }
            if (c->wbuf.writable() < pong.size()) {
                c->pending_bytes.fetch_sub(
                    pong.size(), std::memory_order_relaxed);
                close_conn(c, "wbuf full on pong", false); return;
            }
            if (c->closing.load(std::memory_order_acquire)) {
                // Mid-close: the FIN is already (or about to be) behind
                // everything we owe the peer. Queueing a PONG after it would
                // either be sent past the FIN -- which the kernel refuses --
                // or silently lost. The peer is entitled to nothing more than
                // the reply it asked for.
                c->pending_bytes.fetch_sub(
                    pong.size(), std::memory_order_relaxed);
                server_->stats_ref().frames_dropped_on_close.fetch_add(
                    1, std::memory_order_relaxed);
                c->rbuf.consume(len);
                continue;
            }
            c->wbuf.write(pong.data(), pong.size());
            // frames_sent is likewise counted on leaving wbuf (see
            // submit_send). Idempotent move toward a target state; no
            // "was_empty" test either.
            arm_write(c);
            // close_conn only marks and unlists now and runs no callback on
            // this stack -- though a failed arm just above may already have
            // closed it.
            if (c->is_closed.load(std::memory_order_acquire)) return;
            c->rbuf.consume(len);
        } else if (type == frame_type::PONG) {
            c->rbuf.consume(len);
        } else {
            if (len > 0) {
                // Unreachable, and kept: the alternative to having this branch
                // is an out-of-bounds read, and "it cannot happen" is a claim
                // about today's invariants, not about the next edit. The
                // invariant is that frame_scratch_ is resized to
                // MAX_FRAME_PAYLOAD in start() -- which fails outright if that
                // allocation fails -- and len was clamped to MAX_FRAME_PAYLOAD
                // above, so len <= frame_scratch_.size() always holds. It reads
                // like a defence that fires. It does not.
                if (len > frame_scratch_.size()) {
                    close_conn(c, "scratch too small", false); return;
                }
                c->rbuf.peek(frame_scratch_.data(), len);
            }
            c->rbuf.consume(len);
            auto& mh = server_->msg_handler_ref();
            if (mh) mh(c->id, frame_scratch_.data(), len);
        }
    }

    // A header that has not arrived in full is also a frame in progress.
    if (c->rbuf.readable() > 0 &&
        c->rbuf.readable() < FRAME_HEADER_SIZE &&
        c->frame_started_at_ms == 0) {
        c->frame_started_at_ms = now_ms();
    }
}

// Single teardown path for "this fd must no longer be visible to this reactor".
// Registration failures used to roll back by hand, and missed pieces: one path
// closed the descriptor but left the epoll registration behind, so a recycled
// fd number could arrive with stale interest attached. Every abort/close path
// goes through here so those three steps can never drift apart again.
//
// graceful decides one thing only: whether the close asks for an RST. An
// orderly close relies on the FIN that the caller already sent via
// shutdown(SHUT_WR).
inline void sub_reactor::unregister_conn(connection* c,
                                         bool graceful) noexcept {
    if (c->fd < 0) return;
    // An abort asks for the reset explicitly instead of relying on whatever the
    // close() picks: SO_LINGER{1,0} drops the send buffer and sends RST. It is
    // set on abort only -- never on close normally, or every orderly close
    // would become a reset.
    if (!graceful) {
        struct linger lg{};
        lg.l_onoff  = 1;
        lg.l_linger = 0;
        setsockopt(c->fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    }
    fd_index_.erase(c->fd);
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, c->fd, nullptr);
    ::close(c->fd);
    c->fd = -1;
    c->write_armed = false;
    // No reader observes this today, which is the point of saying so: every
    // reader of `closing` is either iterating conns_ (so the connection is
    // there) or checks is_closed first, and both callers reach this line with
    // is_closed already true -- teardown_closed() sets it before unregistering,
    // and discard_rejected_conn() is erasing the connection as it goes.
    // Clearing it does not un-park anything and nothing may rely on it having
    // done so: what it buys is that a reader added later cannot find `closing`
    // stale-true on a descriptor that is gone, which is a small mercy next to
    // the alternative.
    c->closing.store(false, std::memory_order_release);
}

// A connection that was accepted and then refused by its own registration --
// the map insert threw, or on_connection_opened said no. unregister_conn()
// already took the descriptor, the epoll registration and the fd_index_ entry,
// and used to be the whole of the rollback: the conns_ entry survived it as a
// conn with fd == -1, not closed and not closing. do_tick then read it as idle
// and parked a graceful close whose try_begin_write_shutdown() cannot even
// start (fd < 0), so it died only when the linger expired -- idle_timeout_ms +
// close_linger_ms after it was already dead -- holding a map entry and an
// admission slot meanwhile.
//
// Not teardown_closed(): that counts closed (and aborted), and this connection
// was never accepted, so counting it here would put accepted and closed out of
// step for good. It is the same single-exit reasoning, though -- one function,
// so the two rollback paths cannot drift apart the way they did.
inline void sub_reactor::discard_rejected_conn(connection* c) noexcept {
    unregister_conn(c);
    conns_.erase(c->id);
    server_->on_connection_closed(c->id);
    c->is_closed.store(true, std::memory_order_release);
    // The task holds the last reference and would release the ticket on return
    // either way; releasing it here instead frees the admission slot now rather
    // than whenever the ghost was going to be reaped.
    c->admission.reset();
}

// pending_bytes is reserved at enqueue time and released on two paths only: a
// successful sendmsg (drain_wbuf_to_socket) and an explicit whole-frame drop
// (capacity reject in submit_send / PONG). A closing connection takes a third,
// previously missing path: the bytes still sitting in wbuf are never sent, so
// they must be subtracted here or the reservation leaks for the lifetime of the
// connection -- and any watermark built on it stays permanently inflated.
//
// Bytes still reserved by queued-but-unwritten send tasks are NOT handled here:
// submit_send already releases them on the is_closed path, so this function
// must never try to guess at them (it would double-count).
inline void sub_reactor::discard_pending_output(connection* c) noexcept {
    const size_t remaining = c->wbuf.readable();
    if (remaining == 0) return;
    c->wbuf.clear();
    // The in-flight frame boundary dies with the buffer.
    c->send_frame_remain = 0;
    c->pending_bytes.fetch_sub(remaining, std::memory_order_relaxed);
    server_->stats_ref().bytes_dropped.fetch_add(
        remaining, std::memory_order_relaxed);
}

// Close is split in two on purpose. close_conn() only does the work that is
// safe to do inside a handler stack frame: unlist, detach the descriptor, drop
// both maps' references, bump counters. It never destroys the connection (the
// caller holds a shared_ptr) and never invokes user callbacks. A graceful close
// is not even that much: it parks the connection and may not unlist it for a
// while.
//
// USER CALLBACKS ARE DEFERRED. conn_state_handler(false) / close_handler may
// call stop() (allowed) or release the last external owner of
// multi_reactor_server; if they ran here, the stack frame would resume on a
// destructed object -- this reactor in particular, since cleanup()/stop()
// continue running from close_conn.
// flush_close_callbacks() drains them from a frame that owns no reference to
// the closed connection, after the caller's keep_alive is gone.
//
// Two ways out of a connection, and the difference is visible on the wire:
//   close_conn(..., graceful=true)  -- default. Called when the peer, the
//       application or the server asked to end an otherwise healthy connection.
//       Everything owed to the peer is delivered first: see park_close().
//   close_conn(..., graceful=false) -- abort. Pending output is dropped and
//       SO_LINGER{on,0} asks the kernel for an RST, so the peer cannot
//       mistake a half-processed stream for a complete one.
inline void sub_reactor::close_conn(connection* c, const char* reason,
                                  bool graceful) noexcept {
    // A graceful close of a live connection is parked; one that has already
    // sent its FIN (or is aborted) is torn down right here.
    if (graceful && !c->fin_sent) {
        park_close(c, reason); return;
    }
    teardown_closed(c, reason, graceful);
}

// Why the close cannot simply be "write what is queued, then close":
//   1. wbuf lives in user space. shutdown() takes effect immediately and closes
//      the kernel's send direction, so anything still in wbuf when it is called
//      is lost for good.
//   2. close() on a socket whose peer data is unread sends RST and discards the
//      queued output. The peer's last request is often still unread here.
// So: drain wbuf into the kernel first, then shutdown(SHUT_WR); the kernel
// keeps delivering what it already accepted, in order, before the FIN. The
// socket is closed only after the peer's FIN arrives (do_tick bounds the
// wait).
//
// A graceful close while the connection is still alive on the wire. Until the
// peer has seen both everything we owed it and our FIN the connection is marked
// `closing`, so no further submissions are accepted -- but the reply queued
// just before the close still is (see submit_send).
//
// Idempotent: a second close request (or an idle-timeout sweep firing while the
// first FIN is still in flight) must not restart the drain or skip it.
inline void sub_reactor::park_close(connection* c,
                                    const char* reason) noexcept {
    if (c->is_closed.load(std::memory_order_acquire) || c->fin_sent) return;
    if (!c->closing.load(std::memory_order_acquire)) {
        c->closing.store(true, std::memory_order_release);
        c->close_started_at_ms = now_ms();
    }
    // Hand over what is already queued; whatever does not fit in the socket
    // buffer stays in wbuf and is finished by handle_write (EPOLLOUT armed).
    const drain_outcome r = drain_wbuf_to_socket(c);
    if (r == drain_outcome::error) {
        teardown_closed(c, reason, false); return;
    }
    // Once the socket refuses writes for good there is no graceful close left
    // to honour -- nothing owed can leave, so no FIN can honestly follow it.
    // Waiting would change nothing except which deadline does this same abort
    // later.
    if (r == drain_outcome::dead) {
        teardown_closed(c, reason, false); return;
    }
    if (!c->wbuf.empty()) {
        arm_write(c); return;
    }
    // wbuf empty: either every byte is in the socket buffer, or sendmsg stopped
    // exactly on the budget boundary. In both cases nothing more can be queued
    // (`closing` refuses submissions), so the FIN goes out now.
    try_begin_write_shutdown(c);
}

// close_connection() asked to hang up while the peer's request may still be
// unread in the socket buffer. Those bytes arrived before this close did, so
// answers to them are part of what "graceful" promises the peer: read and serve
// them, then FIN.
//
// Skipping this still parses every frame and still runs the handler, so the
// application believes it answered -- then submit_send refuses the replies
// because `closing` is set, counts them into bytes_dropped, and the peer
// receives an orderly FIN over requests nobody ever answered. Nothing says so
// out loud: frames_recv goes up, the handler ran, and only the byte count hints
// that a request was taken in and served nothing.
//
// The FIN cannot go out from this frame even though the drain just finished.
// Those replies are queued as tasks, behind this one, so wbuf reads empty here
// and park_close would half-close ahead of them. Resuming one queue round later
// is what puts the answers in front of the FIN. It resumes at most once: a peer
// that keeps sending would otherwise hold the close open with more input.
//
// "served" is not "the backlog is done". handle_read stops at k_frame_budget
// frames or k_read_budget bytes per call and reschedules itself for the rest,
// so a peer that sent more than one budget's worth has input left in rbuf when
// this returns -- and parking now sets `closing`, which makes submit_send
// refuse every reply those frames produce. The peer's requests were taken in,
// handled, and answered into nothing: the exact failure this function exists to
// prevent, reached through the read budget instead of through queue ordering
// (measured: 600-frame burst -> 512 answered, 88 dropped; 1200 -> 512
// answered).
//
// So when input is still buffered this comes back for another round instead of
// parking. The rounds are bounded by close_input_bytes, measured once, from
// what was already buffered when the close was asked for: fresh arrivals do not
// extend it, so a peer that keeps sending cannot hold the close open.
inline void sub_reactor::drain_input_then_close(connection* c) noexcept {
    if (c->is_closed.load(std::memory_order_acquire) || c->fin_sent) return;

    // Measured before the read: what had arrived and had not been served yet.
    if (c->close_input_bytes < 0)
        c->close_input_bytes = static_cast<int64_t>(c->rbuf.readable());

    c->drain_read_bytes = 0;
    const size_t queued_before = c->rbuf.readable();
    const int served = handle_read(c);
    // handle_read has its own exits: the peer's FIN may have arrived in the
    // same read, or the socket may have errored. Either way the connection is
    // already accounted for and there is nothing left for this request to do.
    if (c->is_closed.load(std::memory_order_acquire) || c->fin_sent) return;

    // Charged in bytes, and for every byte the call handed out -- including the
    // ones that arrived during it. Charging `served * FRAME_HEADER_SIZE` made
    // this a frame counter that every frame paid the same flat eight bytes for
    // whatever it carried, so the credit was really "backlog / 8 frames", each
    // of which may be MAX_FRAME_PAYLOAD wide: a read buffer holding 1.7 KiB let
    // a peer that kept talking be served 13.7 MiB and 210 frames after the
    // application had asked to hang up. Nothing bounded it either -- the close
    // had not parked, so no linger deadline existed yet, and every arrival
    // refreshed the idle clock.
    //
    // Charging net of what is still buffered is not enough either: a peer that
    // keeps the pipe full has its bytes consumed by the same read that fetched
    // them, so "what is left in rbuf" barely moves however much it sends. Bytes
    // served = what was buffered, plus what this call read, minus what is still
    // there, and that is FIFO: the backlog is charged off the head first, and
    // everything the peer adds afterwards is charged against whatever is left
    // of it. What survives past that is at most one read budget of the newest
    // input, bounded by k_read_budget and independent of how much the peer
    // sends.
    if (served > 0) {
        const int64_t dealt = static_cast<int64_t>(queued_before)
                            + static_cast<int64_t>(c->drain_read_bytes)
                            - static_cast<int64_t>(c->rbuf.readable());
        if (dealt > 0) c->close_input_bytes -= dealt;
        c->drain_read_bytes = 0;
    }

    // More of the owed input is still buffered and the budget allows another
    // round: come back for it. Parking here would refuse the replies of frames
    // this close promised to serve. Progress is required (served > 0) so a peer
    // that stops mid-frame cannot keep the close pending on a half a frame.
    if (served > 0 && !c->rbuf.empty() && c->close_input_bytes > 0) {
        const uint64_t id = c->id;
        if (enqueue(queue_task{drain_input_task{id}})) return;
        // The hand-off failed: park below rather than keeping it pending.
    }

    // Nothing further to read, yet bytes are still reserved: replies whose
    // handler already ran -- possibly two batches apart, with this task landing
    // between them. Parking now side-effects every one of them into `closing`,
    // which submit_send refuses, and the peer gets an orderly FIN over a
    // request we took in, handled, and answered into nothing (case F: 11 of 22
    // bytes).
    // Their reservations still standing is what makes them visible here.
    if (served == 0 &&
        c->deferred_round == connection::deferred_round_kind::none &&
        c->pending_bytes.load(std::memory_order_relaxed) > 0) {
        c->deferred_round = connection::deferred_round_kind::close;
        const uint64_t id = c->id;
        if (enqueue(queue_task{park_close_task{id}})) return;
        // The hand-off failed: park here rather than keeping it forever.
    }
    // The peer's FIN arrived in the same read that dispatched these frames, and
    // finish_peer_closed() already claimed the round: it is queued to come back
    // once the replies have landed in wbuf. Parking here would set `closing`
    // and -- wbuf still being empty -- send the FIN at once, over the very
    // replies that deferral exists to protect. The close ends in the task that
    // owns the round, which is also the only one that can see whether anything
    // is still owed.
    if (c->deferred_round ==
        connection::deferred_round_kind::peer_closed) return;

    if (served == 0 ||
        c->deferred_round == connection::deferred_round_kind::close) {
        park_close(c, "requested");
        return;
    }

    c->deferred_round = connection::deferred_round_kind::close;
    const uint64_t id = c->id;
    if (enqueue(queue_task{park_close_task{id}})) return;

    // Nothing came of the hand-off -- queue full, or the reactor is going down.
    // A late close beats an answer nobody would wait for, so honour it here.
    park_close(c, "requested");
}

// Unlist -> detach descriptor -> account -> queue deferred callbacks. Shared by
// the graceful and abort exits so the two can never disagree on counters.
// `graceful=false` closes with SO_LINGER{on,0}: an explicit RST instead of the
// FIN default, so a peer is never handed a truncated stream that looks
// complete.
inline void sub_reactor::teardown_closed(connection* c, const char* reason,
                                       bool graceful) noexcept {
    bool expected = false;
    if (!c->is_closed.compare_exchange_strong(expected, true,
                                             std::memory_order_acq_rel))
        return;
    // reason is not stored on the connection: it is forwarded straight into the
    // deferred callback below, which copies it. Keeping it here too buys
    // nothing and lasts exactly as long as the caller's string does -- a trap
    // for the next person to add a reader after the teardown has returned.
    unschedule_read(c);
    // Anything still in wbuf will never be sent. Reconcile the byte reservation
    // with the buffer that backs it, or pending_bytes never drains for this
    // connection and any watermark derived from it leaks. Accounted before the
    // close so a close callback reading stats sees it.
    discard_pending_output(c);

    std::shared_ptr<connection> keep_alive;
    {
        auto it = conns_.find(c->id);
        if (it != conns_.end()) {
            keep_alive = it->second; conns_.erase(it);
        }
    }
    // fd_index_ erase + EPOLL_CTL_DEL + close + fd = -1
    unregister_conn(c, graceful);

    server_->stats_ref().closed.fetch_add(1, std::memory_order_relaxed);
    if (!graceful)
        server_->stats_ref().aborted.fetch_add(
            1, std::memory_order_relaxed);
    server_->on_connection_closed(c->id);
    c->admission.reset();

    // Queue the deferred notification: one entry, one insertion.
    //
    // This used to be four push_backs into four parallel vectors, and every
    // function on that path is noexcept -- teardown_closed, close_conn, and
    // everything that calls them. A bad_alloc on the second one left the
    // first behind, so the next batch fed a callback the id of one connection
    // with the peer string of another, and the exception itself terminated
    // the process. On the close path, which is the one that gets walked when
    // memory is tight.
    //
    // One object cannot be half-inserted. The catch is not about making the
    // notification succeed -- the connection is already torn down and gone by
    // here -- it is about not turning an allocation failure into a crash, and
    // about the loss being countable rather than silent.
    try {
        closed_conn entry;
        entry.conn   = std::move(keep_alive);
        entry.id     = c->id;
        entry.peer   = c->peer;
        entry.reason = reason ? reason : "";
        pending_closes_.push_back(std::move(entry));
    } catch (...) {
        server_->stats_ref().close_callbacks_dropped.fetch_add(1,
            std::memory_order_relaxed);
    }
}

// Release our reference before running the callbacks: after this the connection
// may be fully destroyed, which is exactly the state a deferred callback
// expects.
// Takes a mutable reference on purpose: reset() cannot be called through const,
// and the caller's slot must be emptied rather than merely read.
inline void sub_reactor::finish_close(std::shared_ptr<connection>& c) noexcept {
    c.reset();
}

// Runs the deferred conn_state_handler(false) / close_handler pairs. The batch
// is swapped out first, so a callback that closes more connections (or enqueues
// work) lands in a fresh batch instead of invalidating this loop.
inline void sub_reactor::flush_close_callbacks() noexcept {
    std::vector<closed_conn> batch;
    batch.swap(pending_closes_);
    if (batch.empty()) return;

    for (auto& e : batch) {
        finish_close(e.conn);              // connection may die here
        auto& sh = server_->state_handler_ref();
        if (sh) sh(e.id, e.peer, false);
        if (server_->close_handler_)
            server_->close_handler_(e.id, e.peer, e.reason.c_str());
    }
}

inline void sub_reactor::schedule_read(connection* c) noexcept {
    if (c->read_pending) return;
    c->read_pending = true;
    c->read_prev = read_tail_;
    if (read_tail_) read_tail_->read_next = c;
    else read_head_ = c;
    read_tail_ = c;
}

inline void sub_reactor::unschedule_read(connection* c) noexcept {
    if (!c->read_pending) return;
    if (c->read_prev) c->read_prev->read_next = c->read_next;
    else read_head_ = c->read_next;
    if (c->read_next) c->read_next->read_prev = c->read_prev;
    else read_tail_ = c->read_prev;
    c->read_prev = c->read_next = nullptr;
    c->read_pending = false;
}

inline void sub_reactor::do_tick() noexcept {
    const int64_t now   = now_ms();
    const int64_t cutoff = now - server_->idle_timeout_ms();
    const int64_t linger = server_->close_linger_ms();
    const int64_t frame_timeout =
        server_->cfg_.resolved_frame_assembly_timeout_ms();
    // Every due close is collected during the walk and performed after it,
    // because close_conn is not always content to park the connection. Three
    // ways in reach from a single call all erase from conns_ -- the map this
    // range-for is holding an iterator into:
    //   - a connection that has already sent its FIN goes straight to
    //     teardown_closed (park_close returns early on fin_sent);
    //   - park_close's own drain may fail (a peer that vanished and RST'd) and
    //     takes the same teardown;
    //   - arm_write/disarm_write abort the connection if epoll_ctl fails.
    // none of them is reachable from the sweep today on purpose; all of them
    // are reachable by accident. Moving the closes out of the loop makes the
    // question moot instead of documented, which is stronger than the assert
    // that would otherwise stand in for it -- an assert is compiled out under
    // NDEBUG, leaving production with nothing.
    //
    // The connections are carried as shared_ptr: erase detaches them from
    // conns_ between being chosen and being closed, and nothing else holds a
    // reference across that gap.
    using connection_ptr = std::shared_ptr<connection>;
    std::vector<connection_ptr> idle;   // graceful: may still owe bytes
    std::vector<connection_ptr> expired;   // past its deadline: forced
    std::vector<connection_ptr> stalled;   // frame never completed
    for (auto& kv : conns_) {
        connection* c = kv.second.get();
        if (c->closing.load(std::memory_order_acquire)) {
            // A closing connection has a deadline regardless of which stage it
            // reached, and it is the same value for both: how long the close
            // may take. last_recv_ms is useless here (the peer has nothing left
            // to send), so measure from the close itself; park_close stamps
            // close_started_at_ms before any of the stages that can stall.
            if (linger > 0 && c->fd >= 0 && c->close_started_at_ms != 0 &&
                now - c->close_started_at_ms >= linger)
                expired.push_back(kv.second);
            continue;
        }
        if (c->last_recv_ms < cutoff) {
            server_->stats_ref().idle_timeouts.fetch_add(
                1, std::memory_order_relaxed);
            // Default (graceful): an idle connection is still a legitimate peer
            // and may hold a half-finished exchange, so let the close park
            // until what we owe it is out.
            idle.push_back(kv.second);
            continue;
        }
        // Bytes keep arriving but the frame never completes. This is the case
        // idle_timeout_ms deliberately does not catch; abort rather than park
        // so the slot is released now.
        if (c->frame_started_at_ms != 0 && frame_timeout > 0 &&
            now - c->frame_started_at_ms >= frame_timeout) {
            server_->stats_ref().frame_assembly_timeouts.fetch_add(
                1, std::memory_order_relaxed);
            stalled.push_back(kv.second);
        }
    }
    for (auto& c : idle)    close_conn(c.get(), "idle timeout");
    for (auto& c : expired) close_conn(c.get(), "close linger expired", false);
    for (auto& c : stalled)
        close_conn(c.get(), "frame assembly timeout", false);
}

inline void sub_reactor::publish_stats() noexcept {
    server_stats& stats = server_->stats_ref();
    if (frames_recv_local_ != 0) {
        stats.frames_recv.fetch_add(
            frames_recv_local_, std::memory_order_relaxed);
        frames_recv_local_ = 0;
    }
    if (frames_sent_local_ != 0) {
        stats.frames_sent.fetch_add(
            frames_sent_local_, std::memory_order_relaxed);
        frames_sent_local_ = 0;
    }
    if (bytes_recv_local_ != 0) {
        stats.bytes_recv.fetch_add(
            bytes_recv_local_, std::memory_order_relaxed);
        bytes_recv_local_ = 0;
    }
    if (bytes_sent_local_ != 0) {
        stats.bytes_sent.fetch_add(
            bytes_sent_local_, std::memory_order_relaxed);
        bytes_sent_local_ = 0;
    }
}

inline void sub_reactor::loop() noexcept {
    current_ = this;
    multi_reactor_server::callback_scope callbacks(server_);
    try {
        std::array<struct epoll_event, 1024> events{};
        auto last_tick = std::chrono::steady_clock::now();
        const int tick_ms = server_->tick_ms();

        while (running_.load(std::memory_order_acquire) &&
               server_->is_running()) {
#ifdef INDUSTRIAL_TCP_TESTING
            if (server_->fail_worker_for_test.exchange(false)) throw 1;
#endif
            // Only visit queued continuations. A fixed batch bounds work even
            // when a busy connection immediately schedules itself again.
            for (size_t budget = 0; read_head_ && budget < 64; ++budget) {
                auto it = conns_.find(read_head_->id);
                if (it == conns_.end()) {
                    unschedule_read(read_head_); continue;
                }
                auto c = it->second;
                handle_read(c.get());
            }
            publish_stats();
            int n = ::epoll_wait(epoll_fd_, events.data(),
                                 static_cast<int>(events.size()),
                                 read_head_ ? 0 : tick_ms);
            if (n < 0) {
                if (errno == EINTR) continue;
                server_->stats_ref().reactor_failures.fetch_add(
                    1, std::memory_order_relaxed);
                server_->fail();
                break;
            }
            for (int i = 0; i < n; ++i) {
                int      fd = events[i].data.fd;
                uint32_t ev = events[i].events;

                if (fd == wake_fd_) {
                    drain_wakeup(); process_pending(); continue;
                }

                auto it = fd_index_.find(fd);
                if (it == fd_index_.end()) continue;
                connection* c = it->second;

                // ---- hold a shared_ptr alive across the whole event dispatch
                // ----
                std::shared_ptr<connection> keep_alive;
                {
                    auto cit = conns_.find(c->id);
                    if (cit != conns_.end()) keep_alive = cit->second;
                }
                if (!keep_alive) continue;

                // HUP/ERR is the kernel telling us the socket is already broken
                // (or the peer is gone for good); draining into it cannot work.
                // Abort so the peer is not left waiting on a stream that will
                // never complete.
                if (ev & (EPOLLHUP | EPOLLERR)) {
                    close_conn(c, "hup/err", false); continue;
                }
                if (ev & EPOLLIN) {
                    handle_read(c);
                    if (c->is_closed.load(std::memory_order_acquire)) continue;
                }
                if (ev & EPOLLOUT) {
                    handle_write(c);
                }
            }
            // Deferred close callbacks run with no per-connection stack frames
            // alive, so a callback may stop() or destroy the server safely.
            flush_close_callbacks();
            process_pending();
            flush_close_callbacks();

            auto now = std::chrono::steady_clock::now();
            if (now - last_tick >= std::chrono::milliseconds(tick_ms)) {
                last_tick = now;
                do_tick();
            }
        }
        publish_stats();
    } catch (...) {
        server_->stats_ref().reactor_failures.fetch_add(
            1, std::memory_order_relaxed);
        // Raise the failure flag only; the lifecycle state stays untouched so a
        // concurrent start() verdict is not overwritten by a worker crash.
        server_->fail();
    }
    // Reject submissions even when the loop failed rather than being stopped.
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        running_.store(false, std::memory_order_release);
    }
    cleanup();
    current_ = nullptr;
}

// ====================================================================
//                     multi_reactor_server implementation
// ====================================================================

inline bool multi_reactor_server::init_listen_socket() noexcept {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char port_str[16];
    std::snprintf(port_str, sizeof(port_str), "%u", (unsigned)cfg_.port);
    const char* host = (cfg_.bind_addr.empty() || cfg_.bind_addr == "0.0.0.0")
                       ? nullptr : cfg_.bind_addr.c_str();
    struct addrinfo* res = nullptr;
    if (getaddrinfo(host, port_str, &hints, &res) != 0) return false;
    for (auto* p = res; p; p = p->ai_next) {
        int fd = ::socket(p->ai_family,
                          p->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                          p->ai_protocol);
        if (fd < 0) continue;
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
        if (p->ai_family == AF_INET6) {
            int zero = 0;
            setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero));
        }
        if (::bind(fd, p->ai_addr, p->ai_addrlen) < 0) {
            ::close(fd); continue;
        }
        if (::listen(fd, cfg_.backlog) < 0) {
            ::close(fd); continue;
        }
        listen_fd_ = fd;
        // config::port == 0 asks the kernel to choose, and the number it chose
        // lives nowhere except on the socket. Without reading it back, a
        // default-config server starts, returns true, and is unreachable by any
        // client that did not already know the port -- and stats() cannot say
        // so either, because accepted stays 0 for exactly the same reason.
        struct sockaddr_storage bound{};
        socklen_t bound_len = sizeof(bound);
        uint16_t resolved = 0;
        if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&bound),
                          &bound_len) == 0) {
            if (bound.ss_family == AF_INET) {
                resolved = ntohs(
                    reinterpret_cast<struct sockaddr_in*>(&bound)->sin_port);
            } else if (bound.ss_family == AF_INET6) {
                resolved = ntohs(
                    reinterpret_cast<struct sockaddr_in6*>(&bound)->sin6_port);
            }
        }
        // getsockname failing on a descriptor that just bound is not a state
        // worth a third meaning for; port() then reports what was asked for,
        // which is wrong only in the case that cannot happen.
        bound_port_.store(resolved != 0 ? resolved : cfg_.port,
                         std::memory_order_release);
        break;
    }
    freeaddrinfo(res);
    return listen_fd_ >= 0;
}

inline void multi_reactor_server::close_listen_socket() noexcept {
    if (listen_fd_ >= 0) {
        ::close(listen_fd_); listen_fd_ = -1;
    }
    bound_port_.store(0, std::memory_order_release);
}

inline bool multi_reactor_server::start() noexcept {
    if (callback_server_ == this) return false;
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (life_.load(std::memory_order_acquire) != life::stopped) return false;
    life_.store(life::starting, std::memory_order_release);
    failed_.store(false, std::memory_order_release);
    if (!reactors_.empty() || accept_thread_.joinable() ||
        cfg_.rbuf_capacity < MAX_FRAME_SIZE ||
        cfg_.wbuf_capacity < FRAME_HEADER_SIZE ||
        cfg_.max_pending_bytes < FRAME_HEADER_SIZE ||
        cfg_.max_connections <= 0 || cfg_.tick_ms <= 0 ||
        cfg_.idle_timeout_ms <= 0 || cfg_.write_budget_bytes == 0 ||
        cfg_.close_linger_ms <= 0 || cfg_.backlog <= 0 ||
        cfg_.frame_assembly_timeout_ms < 0 ||
        cfg_.handler_threads < 0 ||
        cfg_.handler_threads > config::k_max_handler_threads ||
        (cfg_.handler_threads > 0 && cfg_.handler_queue_limit == 0)) {
        life_.store(life::stopped, std::memory_order_release);
        return false;
    }

    // Deadline granularity: these timeouts only fire on tick boundaries, so a
    // tick coarser than the deadline silently stretches it by up to a full
    // tick. Warn rather than refuse -- a coarse tick with long deadlines is a
    // legitimate choice; a coarse tick with a 200ms promise is a surprise.
    if (cfg_.tick_ms > cfg_.close_linger_ms ||
        cfg_.tick_ms > cfg_.idle_timeout_ms ||
        static_cast<int64_t>(cfg_.tick_ms) >
            cfg_.resolved_frame_assembly_timeout_ms()) {
        std::fprintf(stderr,
                     "[warn] tick_ms=%d is coarser than close_linger_ms=%d / "
                     "idle_timeout_ms=%d / frame_assembly_timeout_ms=%" PRId64
                     ": these deadlines only fire on tick boundaries, so each "
                     "can land up to a full tick late\n",
                     cfg_.tick_ms, cfg_.close_linger_ms, cfg_.idle_timeout_ms,
                     cfg_.resolved_frame_assembly_timeout_ms());
    }

    // Configuration validation
    if (cfg_.max_pending_bytes > cfg_.wbuf_capacity) {
        cfg_.max_pending_bytes = cfg_.wbuf_capacity;
    }

    // ---- fd ceiling: raise it if we can, and say how far it goes if we cannot
    // ----
    // Warning and starting anyway hands operations a server that accepts the
    // first thousand connections and then refuses everything without a word,
    // which is worse than refusing to boot: it looks healthy until it isn't.
    // Raising the soft limit is the one action here that cannot make things
    // worse -- the hard ceiling stays where the environment put it -- so try it
    // before reporting anything.
    struct rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        const uintmax_t needed =
            static_cast<uintmax_t>(cfg_.max_connections) + 64;
        // Compare in uintmax_t, never int: RLIM_INFINITY is "no limit", and
        // narrowing it to int wraps negative, which read as "too low" and
        // printed a spurious warning on unlimited configurations.
        uintmax_t cur = (rl.rlim_cur == RLIM_INFINITY)
                        ? UINTMAX_MAX : static_cast<uintmax_t>(rl.rlim_cur);
        if (cur < needed && rl.rlim_cur < rl.rlim_max) {
            struct rlimit want = rl;
            want.rlim_cur = rl.rlim_max;
            if (setrlimit(RLIMIT_NOFILE, &want) == 0) {
                std::fprintf(stderr,
                    "[warn] raised RLIMIT_NOFILE %ju -> %ju for "
                    "max_connections=%d\n",
                    static_cast<uintmax_t>(rl.rlim_cur),
                    static_cast<uintmax_t>(rl.rlim_max),
                    cfg_.max_connections);
                cur = (rl.rlim_max == RLIM_INFINITY)
                      ? UINTMAX_MAX : static_cast<uintmax_t>(rl.rlim_max);
            }
        }
        if (cur < needed) {
            std::fprintf(stderr,
                "[warn] RLIMIT_NOFILE=%ju (hard %ju) too low for "
                "max_connections=%d: connections beyond roughly %ju will be "
                "refused, counted as accept_errors\n",
                static_cast<uintmax_t>(rl.rlim_cur),
                static_cast<uintmax_t>(rl.rlim_max),
                cfg_.max_connections,
                cur > 64 ? cur - 64 : 0);
        }
    }

    if (!init_listen_socket()) {
        life_.store(life::stopped, std::memory_order_release); return false;
    }

    // ---- running must be visible before any worker thread is constructed ----
    // Every worker asks is_running() whether it should keep working, and every
    // worker asks it the instant its thread starts - long before this function
    // gets anywhere near returning. Publishing running later is therefore a
    // race that each worker loses by default: it reads starting, concludes the
    // server never came up, and returns without ever reaching epoll_wait.
    //
    // Two instances of exactly this have shipped. The accept loop read starting
    // and fell through to its teardown, closing the listening socket while
    // start() still answered true (P0-3). The reactors survived only when they
    // happened to start slowly enough to see this store; a fast one exited with
    // running_ = false and then quietly refused every connection round-robin
    // handed it, which looked like nothing at all from the client side - the
    // socket was accepted and closed again immediately, no logs, no counters
    // anyone thinks to read.
    //
    // Everything above here already fails through the recovery path below and
    // reports false; everything below here is threads that read this value the
    // moment they come into existence. There is nothing left to wait for.
    life_.store(life::running, std::memory_order_release);

    // Same resolution as everywhere else; see config::resolved_reactor_count
    // for why this is not computed here.
    int n_reactors = cfg_.resolved_reactor_count();

    try {
        reactors_.reserve(n_reactors);
    }
    catch (...) {
        close_listen_socket();
        life_.store(life::stopped, std::memory_order_release);
        return false;
    }

    try {
    for (int i = 0; i < n_reactors; ++i) {
        auto r = std::make_unique<sub_reactor>(i, this);
        if (!r->start()) {
            for (auto& rr : reactors_) rr->stop();
            reactors_.clear();
            close_listen_socket();
            life_.store(life::stopped, std::memory_order_release); return false;
        }
        reactors_.push_back(std::move(r));
    }
    } catch (...) {
        for (auto& r : reactors_) r->stop();
        reactors_.clear();
        close_listen_socket();
        life_.store(life::stopped, std::memory_order_release); return false;
    }

    // ---- startup handshake: start() returns once the accept thread reports in
    // ----
    // Success means "the accept loop is armed and no component has failed yet",
    // not "the lifecycle counter is still false". A component that died during
    // bring-up raises failed_ instead of rewriting the lifecycle state, so the
    // verdict returned here cannot be flipped out from under the caller.
    // running itself was published further up, before any worker existed.
    accepting_.store(true, std::memory_order_release);
    try {
        auto ready = std::make_shared<std::promise<bool>>();
        auto fut = ready->get_future();
        accept_thread_ = std::thread([this, ready]() noexcept {
            accept_loop(*ready);
        });
        if (fut.get()) {
            if (!failed_.load(std::memory_order_acquire) &&
                start_handler_pool()) {
                return true;
            }
        }
    } catch (...) {
    }
    accepting_.store(false, std::memory_order_release);
    life_.store(life::stopping, std::memory_order_release);
    stop_handler_pool();
    if (accept_thread_.joinable()) accept_thread_.join();
    for (auto& r : reactors_) r->stop();
    reactors_.clear();
    if (wake_fd_ >= 0) {
        ::close(wake_fd_); wake_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_); epoll_fd_ = -1;
    }
    close_listen_socket();
    failed_.store(false, std::memory_order_release);
    life_.store(life::stopped, std::memory_order_release);
    return false;
}

inline void multi_reactor_server::stop() noexcept {
    if (callback_server_ == this) {
        request_stop(); return;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    // A second stop() (or a stop() after a failed start) must not rerun
    // recovery: it would reopen the submission barrier and join threads that
    // are not there.
    if (life_.load(std::memory_order_acquire) == life::stopped) return;
    life_.store(life::stopping, std::memory_order_release);
    {
        std::array<std::unique_lock<std::shared_mutex>, k_conn_shards> drains;
        for (size_t i = 0; i < k_conn_shards; ++i) {
            drains[i] = std::unique_lock<std::shared_mutex>(
                submission_shards_[i].mutex);
        }
        // Drain callers that passed the running check before shutdown began.
    }

    // Wake the accept thread so it exits (idempotent if stop_gracefully()
    // already did this)
    stop_accepting();

    // The accept thread is gone; the fds it owned are safe to close now
    if (wake_fd_   >= 0) {
        ::close(wake_fd_);   wake_fd_   = -1;
    }
    if (epoll_fd_  >= 0) {
        ::close(epoll_fd_);  epoll_fd_  = -1;
    }
    close_listen_socket();

    // Stop every reactor; is_running() already refuses new send_to /
    // close_connection
    for (auto& r : reactors_) r->stop();
    reactors_.clear();
    stop_handler_pool();

    for (auto& shard : conn_shards_) {
        std::unique_lock<std::shared_mutex> lk(shard.mutex);
        shard.map.clear();
    }
    failed_.store(false, std::memory_order_release);
    life_.store(life::stopped, std::memory_order_release);
}

inline bool multi_reactor_server::stop_gracefully(int timeout_ms) noexcept {
    if (callback_server_ == this) {
        // A reactor thread cannot join itself; request the normal stop and let
        // the controlling thread do the drain.
        request_stop();
        return false;
    }

    // No new connections after this point. The accept thread owns its fds, so
    // joining it is also what makes the listen socket closed.
    stop_accepting();

    const int64_t deadline = now_ms() + (timeout_ms > 0 ? timeout_ms : 0);
    while (now_ms() < deadline) {
        const std::vector<uint64_t> ids = connection_ids();
        if (ids.empty() && admitted_.load(std::memory_order_acquire) == 0) {
            break;
        }
        for (uint64_t id : ids) (void)close_connection(id);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const bool drained = connection_count() == 0 &&
                         admitted_.load(std::memory_order_acquire) == 0;
    stop();
    return drained;
}

inline void multi_reactor_server::wait() noexcept {
    while (is_running())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

inline void multi_reactor_server::stop_accepting() noexcept {
    accepting_.store(false, std::memory_order_release);
    wake_accept_loop();
    if (accept_thread_.joinable()) accept_thread_.join();
}

inline void multi_reactor_server::wake_accept_loop() noexcept {
    // Only touches descriptors that the accept thread itself owns and closes
    // under accept_fd_mutex_, so a stop racing with accept-loop teardown can
    // never write into a recycled fd number.
    std::lock_guard<std::mutex> descriptors(accept_fd_mutex_);
    if (wake_fd_ >= 0) {
        uint64_t v = 1;
        ssize_t r = ::write(wake_fd_, &v, sizeof(v));
        (void)r;
    }
}

inline bool multi_reactor_server::start_handler_pool() noexcept {
    if (cfg_.handler_threads <= 0) return true;
    handler_running_.store(true, std::memory_order_release);
    try {
        handler_threads_.reserve(
            static_cast<size_t>(cfg_.handler_threads));
        for (int i = 0; i < cfg_.handler_threads; ++i) {
            handler_threads_.emplace_back([this] { handler_worker(); });
        }
        return true;
    } catch (...) {
        stop_handler_pool();
        return false;
    }
}

inline void multi_reactor_server::stop_handler_pool() noexcept {
    {
        std::lock_guard<std::mutex> lk(handler_mutex_);
        const bool was = handler_running_.exchange(
            false, std::memory_order_acq_rel);
        if (!was && handler_threads_.empty()) return;
    }
    handler_cv_.notify_all();
    for (auto& t : handler_threads_) {
        if (t.joinable()) t.join();
    }
    handler_threads_.clear();
    {
        std::lock_guard<std::mutex> lk(handler_mutex_);
        handler_queues_.clear();
        handler_active_.clear();
        handler_ready_.clear();
        handler_queued_.store(0, std::memory_order_relaxed);
    }
}

inline void multi_reactor_server::handler_worker() noexcept {
    for (;;) {
        std::function<void()> task;
        uint64_t conn_id = 0;
        {
            std::unique_lock<std::mutex> lk(handler_mutex_);
            handler_cv_.wait(lk, [this] {
                return !handler_running_.load(std::memory_order_acquire) ||
                       !handler_ready_.empty();
            });
            if (!handler_running_.load(std::memory_order_acquire)) return;
            if (handler_ready_.empty()) continue;
            conn_id = handler_ready_.front();
            handler_ready_.pop_front();
            auto it = handler_queues_.find(conn_id);
            if (it == handler_queues_.end() || it->second.empty()) continue;
            task = std::move(it->second.front());
            it->second.pop_front();
            handler_active_.insert(conn_id);
            handler_queued_.fetch_sub(1, std::memory_order_relaxed);
        }
        {
            callback_scope scope(this);
            try {
                task();
            } catch (...) {
                // An offloaded task must not kill a pool worker; the same "do
                // not throw" contract as an inline handler.
            }
        }
        {
            std::lock_guard<std::mutex> lk(handler_mutex_);
            handler_active_.erase(conn_id);
            auto it = handler_queues_.find(conn_id);
            if (it != handler_queues_.end()) {
                if (!it->second.empty()) handler_ready_.push_back(conn_id);
                else handler_queues_.erase(it);
            }
        }
        handler_cv_.notify_one();
    }
}

inline bool multi_reactor_server::offload(uint64_t conn_id,
                                        std::function<void()> fn) noexcept {
    if (!handler_running_.load(std::memory_order_acquire)) return false;
    try {
        std::lock_guard<std::mutex> lk(handler_mutex_);
        if (!handler_running_.load(std::memory_order_acquire)) return false;
        if (handler_queued_.load(std::memory_order_relaxed) >=
            cfg_.handler_queue_limit) {
            return false;
        }
        auto& q = handler_queues_[conn_id];
        const bool was_empty = q.empty();
        q.push_back(std::move(fn));
        handler_queued_.fetch_add(1, std::memory_order_relaxed);
        if (was_empty &&
            handler_active_.find(conn_id) == handler_active_.end()) {
            handler_ready_.push_back(conn_id);
            handler_cv_.notify_one();
        }
        return true;
    } catch (...) {
        return false;
    }
}

inline sub_reactor* multi_reactor_server::pick_reactor() noexcept {
    if (reactors_.empty()) return nullptr;
    size_t i = next_reactor_.fetch_add(1, std::memory_order_relaxed) %
               reactors_.size();
    return reactors_[i].get();
}

inline void multi_reactor_server::accept_loop(
        std::promise<bool>& ready) noexcept {
    bool ok = false;
    try {
    {
    std::lock_guard<std::mutex> descriptors(accept_fd_mutex_);
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ >= 0) {
        wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd_ >= 0) {
            struct epoll_event ev{};
            ev.events = EPOLLIN; ev.data.fd = listen_fd_;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) == 0) {
                ev.data.fd = wake_fd_;
                if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) == 0) {
                    ok = true;
                }
            }
        }
    }
    }

    try {
        ready.set_value(ok);
    } catch (...) {}
    // Descriptor setup failure is the accept loop's own failure: raise the
    // shared flag instead of forcing the whole lifecycle down, so start()
    // reports false through its normal recovery path rather than racing on the
    // state.
    if (!ok) {
        fail(); return;
    }

    std::array<struct epoll_event, 64> events{};
    while (ok && accepting_.load(std::memory_order_acquire) && is_running()) {
#ifdef INDUSTRIAL_TCP_TESTING
        if (fail_accept_for_test.exchange(false)) throw 1;
#endif
        int n = ::epoll_wait(epoll_fd_, events.data(),
                             static_cast<int>(events.size()), 500);
        if (n < 0) {
            if (errno == EINTR) continue;
            stats_.reactor_failures.fetch_add(1, std::memory_order_relaxed);
            fail();
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == wake_fd_) {
                uint64_t v = 0;
                ssize_t r = ::read(wake_fd_, &v, sizeof(v)); (void)r;
                continue;
            }
            if (fd != listen_fd_) continue;

            // bounded accept, so a flood of new connections cannot starve the
            // other fds
            constexpr int k_max_accept_per_round = 64;
            for (int k = 0; k < k_max_accept_per_round; ++k) {
                struct sockaddr_storage ss{};
                socklen_t slen = sizeof(ss);
                int cfd = ::accept4(listen_fd_, (struct sockaddr*)&ss, &slen,
                                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (cfd < 0) {
                    if (errno == EINTR) continue;
                    // Out of descriptors is not an error to swallow and it is
                    // not one to spin on either: the listening socket stays
                    // readable, so returning to epoll_wait immediately means
                    // burning a core rejecting nothing. It is also the one
                    // accept failure that can be predicted at startup and
                    // therefore should be countable -- without a counter the
                    // only symptom is "it stopped accepting and said nothing".
                    if (errno == EMFILE || errno == ENFILE) {
                        stats_.accept_errors.fetch_add(
                            1, std::memory_order_relaxed);
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(1));
                        continue;
                    }
                    break;
                }
                fd_guard cfd_guard(cfd);

                bool reject = false;
                {
                    if (admitted_.load() >=
                        static_cast<size_t>(cfg_.max_connections))
                        reject = true;
                }
                if (reject) {
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                    continue;   // cfd_guard closes it
                }

                int one = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY,  &one, sizeof(one));
                setsockopt(cfd, SOL_SOCKET,  SO_KEEPALIVE, &one, sizeof(one));
                int ka_idle = 30, ka_intvl = 10, ka_cnt = 3;
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPIDLE,
                           &ka_idle, sizeof(ka_idle));
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPINTVL,
                           &ka_intvl, sizeof(ka_intvl));
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPCNT,
                           &ka_cnt, sizeof(ka_cnt));

                try {
                char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
                std::string peer = "unknown";
                if (getnameinfo((struct sockaddr*)&ss, slen, hbuf, sizeof(hbuf),
                                sbuf, sizeof(sbuf),
                                NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
                    try {
                        peer.assign(hbuf);
                        peer.push_back(':');
                        peer.append(sbuf);
                    } catch (...) { peer = "unknown"; }
                }

                sub_reactor* r = pick_reactor();
                if (!r) continue;   // cfd_guard closes it

                uint64_t id = next_conn_id_.fetch_add(
                    1, std::memory_order_relaxed);
                // Submission consumes ownership on both success and rejection.
                // accepted is NOT bumped here: queueing a task is not a
                // connection.
                // The reactor bumps it once the socket is registered in both
                // maps, so a task that fails during registration counts as
                // rejected only.
                auto admission = std::make_shared<admission_ticket>(admitted_);
                if (!r->submit_new_connection(std::move(cfd_guard),
                                            std::move(peer), id,
                                            std::move(admission))) {
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                }
                } catch (...) {
                    // Before handoff, cfd_guard still owns the accepted socket.
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }
    } catch (...) {
        stats_.reactor_failures.fetch_add(1, std::memory_order_relaxed);
        fail();
        try {
            ready.set_value(false);
        } catch (...) {}
    }
    // Close on every exit, including unexpected failure. The same mutex
    // protects stop's wake write, preventing descriptor reuse between its check
    // and write.
    std::lock_guard<std::mutex> descriptors(accept_fd_mutex_);
    if (wake_fd_ >= 0) {
        ::close(wake_fd_); wake_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_); epoll_fd_ = -1;
    }
    close_listen_socket();
}

inline bool multi_reactor_server::on_connection_opened(
    uint64_t id, std::shared_ptr<connection> c) noexcept {
    try {
        conn_shard& shard = conn_shards_[conn_shard_for(id)];
        std::unique_lock<std::shared_mutex> lk(shard.mutex);
        shard.map[id] = std::move(c);
        return true;
    } catch (...) {
        // No counter here either. queue_drops is "a frame was refused at
        // enqueue", and there is no frame in this failure -- the false return
        // is what the reactor counts as rejected, which is the one question
        // this answer can support.
        return false;
    }
}

inline void multi_reactor_server::on_connection_closed(uint64_t id) noexcept {
    conn_shard& shard = conn_shards_[conn_shard_for(id)];
    std::unique_lock<std::shared_mutex> lk(shard.mutex);
    shard.map.erase(id);
}

inline send_result multi_reactor_server::send_to(uint64_t conn_id,
                                                 const void* data,
                                                 size_t len) noexcept {
    if (len > MAX_FRAME_PAYLOAD) return send_result::too_large;
    if (len && !data) return send_result::invalid_argument;
    if (!is_running()) return send_result::closed;

    // Owner-thread fast path: a handler runs on the reactor that owns its
    // connection, so it can write straight into wbuf. This bypasses the global
    // submission/connection locks, the task queue and its eventfd wakeup, and
    // the queued path's frame copy. It does not race stop(): this thread is the
    // reactor stop() has to join.
#ifndef INDUSTRIAL_TCP_NO_SEND_FASTPATH
    if (sub_reactor* local = sub_reactor::current()) {
        send_result fast = send_result::ok;
        if (local->try_send_local(this, conn_id, data, len, &fast)) return fast;
    }
#endif

    const size_t shard = conn_shard_for(conn_id);
    std::shared_lock<std::shared_mutex> submissions(
        submission_shards_[shard].mutex);
    if (!is_running()) return send_result::closed;

    std::shared_ptr<connection> c;
    {
        conn_shard& cs = conn_shards_[shard];
        std::shared_lock<std::shared_mutex> lk(cs.mutex);
        auto it = cs.map.find(conn_id);
        if (it == cs.map.end()) return send_result::not_connected;
        c = it->second;
    }
    // The same two conditions submit_send() will apply on the owner's thread,
    // applied here: `closing` is part of it because a connection whose graceful
    // close has been parked is refused there without exception, so ok here
    // would describe a frame that is going into bytes_dropped on arrival. This
    // is not a shutdown or a disconnection that ok is allowed to be vague about
    // -- it is the close this caller asked for moments ago.
    if (c->is_closed.load(std::memory_order_acquire) ||
        c->closing.load(std::memory_order_acquire)) return send_result::closed;

    std::vector<uint8_t> frame;
    try {
        frame.resize(FRAME_HEADER_SIZE + len);
        encode_frame_into(frame.data(), frame_type::DATA, data, len);
    } catch (...) {
        return send_result::overloaded;
    }

    // Ceiling check on the credit above. See pending_bytes for why this being a
    // best-effort soft limit is what makes the relaxed orders there safe.
    size_t old = c->pending_bytes.load(std::memory_order_relaxed);
    for (;;) {
        if (old > cfg_.max_pending_bytes ||
            frame.size() > cfg_.max_pending_bytes - old) {
            stats_.queue_drops.fetch_add(1, std::memory_order_relaxed);
            return send_result::overloaded;
        }
        if (c->pending_bytes.compare_exchange_weak(
                old, old + frame.size(),
                std::memory_order_relaxed,
                std::memory_order_relaxed)) break;
    }

    sub_reactor* owner = c->owner;
    if (!owner || !owner->submit_send(conn_id, std::move(frame))) {
        c->pending_bytes.fetch_sub(
            FRAME_HEADER_SIZE + len, std::memory_order_relaxed);
        return send_result::overloaded;
    }
    return send_result::ok;
}

inline bool multi_reactor_server::close_connection(uint64_t conn_id) noexcept {
    if (!is_running()) return false;
    const size_t shard = conn_shard_for(conn_id);
    std::shared_lock<std::shared_mutex> submissions(
        submission_shards_[shard].mutex);
    if (!is_running()) return false;
    std::shared_ptr<connection> c;
    {
        conn_shard& cs = conn_shards_[shard];
        std::shared_lock<std::shared_mutex> lk(cs.mutex);
        auto it = cs.map.find(conn_id);
        if (it == cs.map.end()) return false;
        c = it->second;
    }
    // Every exit from here reports itself in the same counter. The loss used to
    // be silent: nothing anywhere recorded that this connection was asked to go
    // away, so it simply waited for idle timeout while the application believed
    // it had hung up. (The name says "dropped" because that is what happens to
    // the request; the connection keeps living until something else ends it.)
    if (!c->owner) {
        stats_.close_requests_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (!c->owner->submit_close(conn_id)) {
        stats_.close_requests_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    // The close is queued. A send_to() from the same reactor must now use the
    // queued path too, so it lands after the close task and keeps the existing
    // ordering semantics.
    c->close_requested.store(true, std::memory_order_release);
    return true;
}

inline uint16_t multi_reactor_server::port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
}

inline size_t multi_reactor_server::connection_count() const noexcept {
    size_t n = 0;
    for (const auto& shard : conn_shards_) {
        std::shared_lock<std::shared_mutex> lk(shard.mutex);
        n += shard.map.size();
    }
    return n;
}

inline std::vector<uint64_t> multi_reactor_server::connection_ids() const {
    std::vector<uint64_t> out;
    for (const auto& shard : conn_shards_) {
        std::shared_lock<std::shared_mutex> lk(shard.mutex);
        out.reserve(out.size() + shard.map.size());
        for (const auto& kv : shard.map) out.push_back(kv.first);
    }
    return out;
}

}  // namespace industrial
