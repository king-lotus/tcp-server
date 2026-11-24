// tcp-server-mt.h
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
#include <climits>

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

// epoll_event.data.u64 tag reserved for a reactor's wake eventfd. Connection
// ids start at 1 (next_conn_id_), so 0 can never name a connection. Events
// carry the id rather than the fd, which is what makes a stale event unable to
// alias a connection that recycled the descriptor number.
constexpr uint64_t REACTOR_WAKE_TAG = 0;

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
    // A non-null payload is assumed when len > 0. len <= MAX_FRAME_PAYLOAD is
    // enforced by send_to, but this is an inline public entry point and nothing
    // here would complain about a caller who did not check: it would just write
    // a length the far side cannot honour.
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

// Per-reactor counters and watchdog state: many readers (reactor_stats() and
// the accept thread) and, for most fields, one writer -- the reactor. The two
// watchdog fields are the exception: the accept thread writes those, because
// the whole point is to observe a reactor that has stopped running. Every field
// is atomic. One cache line each: the counters are touched once per loop
// iteration by their reactor, and adjacent reactors must not share the line.
// The traffic counters are cumulative since construction, like server_stats;
// only the per-run fields are reset by start().
struct alignas(64) reactor_slot {
    std::atomic<uint64_t> frames_recv{0};
    std::atomic<uint64_t> frames_sent{0};
    std::atomic<uint64_t> bytes_recv{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<size_t>   connections{0};
    std::atomic<int64_t>  last_progress_ms{0};
    std::atomic<bool>     dead{false};
    // A reactor that is alive but has stopped completing loop iterations: the
    // accept thread's watchdog raised this, and pick_reactor() stops handing it
    // new connections. Distinct from `dead`, which is a reactor that will not
    // come back at all; a stalled one still owns its connections and still
    // accepts queued work for them, so it is skipped, not torn down.
    //
    // Set and cleared only by the watchdog, which samples at least every
    // min(500 ms, timeout / 2). Clearing is the hysteresis half: one healthy
    // sample is not enough, because a reactor that oscillates around the
    // timeout would otherwise be re-admitted and dropped again on every
    // sample. See check_reactor_stalls().
    std::atomic<bool>     stalled{false};
    // When `stalled` was raised, for the recovery dwell. Only meaningful while
    // `stalled` is true.
    std::atomic<int64_t>  stalled_at_ms{0};
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
        // Allocate into a temporary first, so a bad_alloc leaves the object
        // exactly as it was. The storage is deliberately uninitialised
        // (no parentheses): a vector::resize zero-filled it, committing every
        // page at accept for a connection that may stay idle. Only bytes below
        // size_ are ever read, and commit()/write() are the only ways size_
        // grows.
        std::unique_ptr<uint8_t[]> fresh(new uint8_t[cap]);
        buf_.swap(fresh);
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
        std::memcpy(buf_.get() + tail, p, first);
        if (w > first) std::memcpy(buf_.get(), p + first, w - first);
        size_ += w;
        return w;
    }

    size_t peek(uint8_t* p, size_t n) const noexcept {
        size_t r = std::min(n, size_);
        if (r == 0) return 0;
        size_t first = std::min(r, cap_ - head_);
        std::memcpy(p, buf_.get() + head_, first);
        if (r > first) std::memcpy(p + first, buf_.get(), r - first);
        return r;
    }

    // Same as peek, but starting `skip` bytes into the readable region. The
    // send path uses it to read a frame header that is not at the head.
    size_t peek_at(size_t skip, uint8_t* p, size_t n) const noexcept {
        if (skip >= size_) return 0;
        size_t r = std::min(n, size_ - skip);
        size_t at = (head_ + skip) % cap_;
        size_t first = std::min(r, cap_ - at);
        std::memcpy(p, buf_.get() + at, first);
        if (r > first) std::memcpy(p + first, buf_.get(), r - first);
        return r;
    }

    // Room at the write end, for a caller that fills the buffer in place
    // instead of copying through a stack buffer first. Only the contiguous part
    // is offered; a ring that has wrapped has a second piece at buf_[0].
    uint8_t* write_ptr() noexcept {
        if (cap_ == 0) return nullptr;
        return buf_.get() + write_tail();
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
        s.p1 = buf_.get() + head_;
        s.n1 = first;
        if (size_ > first) {
            s.p2 = buf_.get(); s.n2 = size_ - first; s.count = 2;
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

    std::unique_ptr<uint8_t[]> buf_;
    size_t cap_  = 0;
    size_t head_ = 0;
    size_t size_ = 0;
};

// ==================== Statistics ====================
struct server_stats {
    // Connections fully registered with a reactor: in both maps, counted by
    // connection_count(), sendable. Counted only after registration succeeds,
    // so accepted minus closed is the live set.
    std::atomic<uint64_t> accepted{0};
    // Sockets turned away. Wider than "the admission ceiling was reached": an
    // epoll_ctl or map-insert failure, an on_connection_opened() that declines
    // the connection, and an allocation failure in the new-connection task land
    // here too, each beside its own counter. Read it as "a client did not get
    // in", not as a capacity gauge.
    std::atomic<uint64_t> rejected{0};
    // Rejections with a narrower cause than `rejected`, counted beside it:
    // the global buffer budget had no room, or the peer's address already had
    // max_connections_per_ip live connections.
    std::atomic<uint64_t> buffer_budget_rejections{0};
    std::atomic<uint64_t> ip_rejections{0};
    // Buffer bytes reserved by admitted connections (queued and live). Gauge,
    // like pending_tasks_depth; admission is bounded by it when
    // max_total_buffer_bytes is set.
    std::atomic<size_t> buffer_bytes{0};
    // Every connection that left, drained and FINed or aborted alike.
    std::atomic<uint64_t> closed{0};
    // Counted when a frame is parsed and consumed. A frame rejected from its
    // header -- bad magic, over-length, unknown type -- is counted in its
    // rejection counter instead and never reaches this one. Reactors batch the
    // count in a local line-aligned tally and publish it once per loop
    // iteration (and once on loop exit), so it is exact at idle and can lag one
    // iteration while traffic flows.
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
    // Refused submissions. Mostly frames refused for exceeding
    // max_pending_bytes or because a reactor queue is full, but enqueue()
    // cannot tell task types apart, so control tasks (new connection, close,
    // continuation) refused by the same queues land here too.
    alignas(64) std::atomic<uint64_t> queue_drops{0};
    // bytes accepted but never sent: a frame dropped whole (capacity reject,
    // or arriving after the close began), or output still buffered when the
    // connection was torn down.
    std::atomic<uint64_t> bytes_dropped{0};
    // forced disconnects (RST), not a graceful close
    std::atomic<uint64_t> aborted{0};
    // frames dropped because a close was already in progress: the peer gets the
    // orderly FIN, not an answer to these.
    std::atomic<uint64_t> frames_dropped_on_close{0};
    // The stream could not be framed: a bad magic or an over-length frame. The
    // remedy is the same for both, which is why they share a counter -- look at
    // the link, not at the peer's protocol version.
    std::atomic<uint64_t> parse_errors{0};
    // A header with a valid magic carried a type outside {DATA, PING, PONG}
    // while allow_unknown_frame_types was false, and the connection was closed
    // from that header, before the declared payload arrived. Kept apart from
    // parse_errors on purpose: the stream was intact and the peer's protocol
    // version was not, so the operator looks in a different place.
    std::atomic<uint64_t> unknown_frame_types{0};
    std::atomic<uint64_t> idle_timeouts{0};
    // A partial frame did not complete within frame_assembly_timeout_ms. Unlike
    // idle_timeouts, this fires while bytes are still arriving.
    std::atomic<uint64_t> frame_assembly_timeouts{0};
    std::atomic<uint64_t> epoll_ctl_errors{0};
    // A reactor thread crashed or exited abnormally. The reactor is
    // quarantined -- it takes no new connections and its connections are
    // reset -- and the server keeps serving while at least one other reactor
    // is alive. The count that makes the pool empty (or an accept-thread
    // failure) is fatal.
    std::atomic<uint64_t> reactor_failures{0};
    // The accept thread's watchdog observed a live reactor whose last
    // completed loop iteration is older than reactor_stall_timeout_ms. One
    // increment per episode, not per sample: the stall is counted when it is
    // entered, and the episode ends when the reactor has made progress for a
    // full window again. A stall never stops the server; what it does change
    // is routing (see reactor_stall_skips).
    std::atomic<uint64_t> reactor_stalls{0};
    // A new connection that would have been handed to a stalled reactor was
    // handed to a live one instead. Counted once per connection, not once per
    // reactor passed over. Zero whenever the watchdog is off (timeout 0), and
    // it stays zero when every reactor is stalled: the fallback in
    // pick_reactor() then accepts the bad hand-off rather than refusing the
    // connection.
    std::atomic<uint64_t> reactor_stall_skips{0};
    // accept4 failed for want of a file descriptor. The one accept failure
    // predictable at startup, and therefore the one that has to be countable:
    // without it the only symptom is that it stopped accepting connections and
    // never said a word about it.
    std::atomic<uint64_t> accept_errors{0};
    // accept4 failed with EMFILE and the reserved descriptor was spent on the
    // connection the kernel was holding, so the peer gets a reset now rather
    // than waiting in the backlog for a slot that may never come. Counted
    // separately from accept_errors: that one says "accept failed", this one
    // says "a client was turned away".
    std::atomic<uint64_t> fd_exhausted_drops{0};
    // accept4 failed with ENFILE: the system-wide descriptor table is full.
    // Closing one's own spare cannot help there, so it keeps the backoff and
    // its own counter -- the remedy is on the machine, not in this process.
    std::atomic<uint64_t> fd_exhausted_system{0};
    // Slow-path task counters. The owner-thread send_to() fast path does not
    // queue, so it does not touch either.
    alignas(64) std::atomic<uint64_t> pending_tasks_total{0};
    alignas(64) std::atomic<uint64_t> pending_tasks_depth{0};  // queue gauge
    // recv returned 0 (peer's FIN). Under level-triggered epoll a peer that has
    // half-closed makes epoll_wait hand back the same EPOLLIN every time, so
    // this doubles as a probe for whether the reactor is spinning on one FIN:
    // across a parked drain it should barely move.
    std::atomic<uint64_t> eof_reads{0};
    // close_connection() sent, then refused by the queue. The request is not
    // lost: close_wanted hands it to do_tick's sweep, which closes the
    // connection within one tick. The counter remains because a refused
    // hand-off is still a queue-pressure signal, and it is observable where a
    // delayed close is not.
    std::atomic<uint64_t> close_requests_dropped{0};
    // Read-side backpressure engaged: pending output crossed the high
    // watermark and EPOLLIN was dropped. Counts transitions, not bytes.
    std::atomic<uint64_t> read_pauses{0};
    // sendmsg returned EPIPE: the peer sent RST, so this socket will never take
    // another byte. Reaching it takes a narrow window. A RST that is already
    // pending turns the next event into EPOLLHUP/EPOLLERR, which loop() handles
    // before it looks at EPOLLOUT; and once our own FIN is out, the fin_sent
    // guards in handle_write and park_close return before the drain is tried.
    // So this is expected to stay at 0. The counter exists to keep that
    // expectation checkable: one increment means one of those guards has a hole
    // and the write path needs another look.
    std::atomic<uint64_t> epipe_writes{0};
    // Close callbacks dropped: the connection was torn down completely, but the
    // allocation that was to notify the application failed. Expected to stay 0
    // (nothing but bad_alloc reaches it), and it exists because losing a
    // notification has to be readable -- otherwise the only symptom is a peer
    // watching the connection vanish with its callback never invoked and no
    // counter moving at all.
    std::atomic<uint64_t> close_callbacks_dropped{0};
    // A close-side callback (conn_state_handler(false) or close_handler)
    // threw. It used to reach std::terminate because the flush is noexcept;
    // now the exception is contained and counted, so one application bug
    // cannot take every connection down with it.
    std::atomic<uint64_t> callback_exceptions{0};
    // A handler installer (set_message_handler, set_conn_state_handler,
    // set_close_handler) was called while the server was not fully stopped, or
    // the install itself failed, and the handler was left as it was. Handlers
    // are read by every worker, so replacing one while they run is a data race
    // on a std::function, not a reconfiguration. The
    // counter exists because the failure is otherwise silent in the one way
    // that matters: the application swaps a handler, gets nothing back from the
    // call if it ignores the result, and never learns why the old one is still
    // the one running.
    std::atomic<uint64_t> handler_installs_refused{0};
};

// ==================== Send results ====================
enum class send_result : int {
    ok = 0,            // accepted: queued, or already in wbuf
    not_connected,     // no such connection
    closed,            // server stopped, or this connection closing/closed
    too_large,         // len > MAX_FRAME_PAYLOAD
    overloaded,        // max_pending_bytes, queue full, or copy failed
    invalid_argument,  // len > 0 with a null data
};

class multi_reactor_server;
class sub_reactor;

// Live connection count per numeric peer address. The accept thread is the
// only acquirer, but release runs wherever teardown does, so the count is
// mutex-protected. Entries are erased at zero: a table that only ever grew
// would itself be the denial of service it exists to prevent.
struct ip_admission {
    std::mutex mutex;
    std::unordered_map<std::string, size_t> counts;

    bool try_acquire(const std::string& key, int cap) noexcept {
        try {
            std::lock_guard<std::mutex> lk(mutex);
            size_t& n = counts[key];
            if (n >= static_cast<size_t>(cap)) return false;
            ++n;
            return true;
        } catch (...) {
            // Could not allocate the entry: refuse rather than terminate. The
            // rejection is counted by the accept loop.
            return false;
        }
    }
    void release(const std::string& key) noexcept {
        try {
            std::lock_guard<std::mutex> lk(mutex);
            auto it = counts.find(key);
            if (it == counts.end()) return;
            if (--it->second == 0) counts.erase(it);
        } catch (...) {
        }
    }
};

// One reservation follows a socket from the accept queue through connection
// lifetime. Every failure/discard path releases it through the same destructor.
// The RMWs are relaxed, same policy as every other gauge: RMW cannot lose an
// update, nothing non-atomic is published through the count, and the accept
// loop's cap check is advisory by nature (the load races the reservation by
// definition). The ++/-- operator forms would quietly mean seq_cst.
struct admission_ticket {
    std::atomic<size_t>& count;
    std::atomic<size_t>& bytes;
    size_t               reserved;
    std::shared_ptr<ip_admission> ip_table;
    std::string          ip_key;
    bool                 ip_held = false;

    // May allocate (the key copy); the caller is the accept loop's try block.
    admission_ticket(std::atomic<size_t>& value,
                     std::atomic<size_t>& byte_gauge,
                     size_t byte_reservation,
                     std::shared_ptr<ip_admission> table = {},
                     std::string key = {})
        : count(value), bytes(byte_gauge), reserved(byte_reservation),
          ip_table(std::move(table)), ip_key(std::move(key)) {
        count.fetch_add(1, std::memory_order_relaxed);
        bytes.fetch_add(reserved, std::memory_order_relaxed);
    }
    // Takes the per-IP slot, if one is configured. Called after construction;
    // false means the cap is reached and no slot was taken.
    bool acquire_ip(int cap) noexcept {
        if (ip_held) return true;
        if (!ip_table || cap <= 0) return true;
        ip_held = ip_table->try_acquire(ip_key, cap);
        return ip_held;
    }
    admission_ticket(const admission_ticket&) = delete;
    admission_ticket& operator=(const admission_ticket&) = delete;
    admission_ticket(admission_ticket&&) = delete;
    admission_ticket& operator=(admission_ticket&&) = delete;
    ~admission_ticket() {
        count.fetch_sub(1, std::memory_order_relaxed);
        bytes.fetch_sub(reserved, std::memory_order_relaxed);
        if (ip_table && ip_held) ip_table->release(ip_key);
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

// What the close FSM tells the reactor to do next. The FSM decides, the reactor
// performs the effect and feeds the result back as the next event.
enum class close_action {
    none,
    drain_input,        // serve buffered input (run handle_read)
    drain_wbuf,         // move wbuf into the kernel
    drain_complete,     // wbuf drained empty: the close decision is next
    send_fin,           // shutdown(SHUT_WR)
    defer_drain_input,  // queue one more input-serving round
    defer_park,         // queue one park round (replies land first)
    defer_peer_closed,  // queue one peer-closed round
    close_graceful,     // teardown, orderly
    close_abort,        // teardown, RST
};

// ==================== Connection state machine ====================
//
// One state machine owns everything that decides what a connection may do
// next. Before it existed that knowledge was eleven fields on two structs --
// closing, fin_sent, is_closed, deferred_round, draining_input,
// close_input_bytes and close_started_at_ms on close_fsm; write_armed,
// read_paused, peer_closed and paused_at_ms on connection -- written from
// fourteen places, so "may a paused connection be closing?" was a question
// about the call graph. It is now a question about one predicate,
// conn_sm::valid(), which the state-machine test checks after every
// transition from every state it admits, and which every transition asserts.
//
// The state is a product of three regions, because the three really are
// independent -- what is not independent is which combinations may coexist,
// and that is what valid() says:
//
//   phase  open -> draining -> closing -> fin_sent -> closed
//          open:     normal operation.
//          draining: a close was requested (close_connection, stop_gracefully)
//                    and buffered input is still being served; submissions are
//                    still accepted, because the replies are what the close
//                    exists to deliver.
//          closing:  parked. Submissions are refused, wbuf is being drained,
//                    the FIN waits for it.
//          fin_sent: our FIN is out; waiting for the peer's FIN or the linger.
//          closed:   torn down. Terminal; every flag is normalised.
//          open and draining may go straight to closing (idle timeout, and a
//          close that has nothing left to serve); any phase may go to closed.
//   read   active | paused | eof.
//          paused is read-side backpressure (EPOLLIN dropped); eof is the
//          peer's FIN. paused exists only in open/draining, outside a serving
//          round: a close resumes a paused connection as part of entering
//          closing, which is why the machine does it rather than the caller.
//   write  write_armed: EPOLLOUT is wanted.
//
// Extended state, kept with the machine because it is what the close protocol
// decides with: in_round (a serving round is on the stack), round (which side
// owns the one queue round a close or a peer FIN may spend waiting for replies
// to land), close_input_bytes (the input a graceful close still owes the peer,
// measured once), close_started_at_ms, paused_at_ms.
//
// Threading: only the owning reactor mutates the state. The phase is mirrored
// into an atomic on every transition, which is the only part other threads
// read (send_to, close_connection); close_requests/close_wanted are the
// cross-thread request channel into the machine and are atomics of their own.
//
// The machine decides and the reactor performs: events return a close_action
// (and, where entering a phase implies resuming reads, a read_resume for the
// reactor to apply), and the reactor feeds the outcome back as the next event.
//
// Events and their preconditions (asserted; "none" means any state):
//   on_pause, on_resume, on_eof, on_set_write, on_graceful, on_request,
//   on_round_end, on_closed ........ none (each guards itself)
//   on_round_begin ................. reads not paused (on_request resumed them)
//   on_input_drained, _defer_failed  a close was requested (budget measured)
//   on_peer_fin .................... read == eof
//   on_fin_sent .................... phase == closing
enum class conn_phase : uint8_t {
    open = 0, draining, closing, fin_sent, closed
};
// Published phases are compared by order (refuses_sends): keep it meaningful.
static_assert(static_cast<int>(conn_phase::open) <
              static_cast<int>(conn_phase::draining) &&
              static_cast<int>(conn_phase::draining) <
              static_cast<int>(conn_phase::closing) &&
              static_cast<int>(conn_phase::closing) <
              static_cast<int>(conn_phase::fin_sent) &&
              static_cast<int>(conn_phase::fin_sent) <
              static_cast<int>(conn_phase::closed),
              "conn_phase order is relied on");

enum class read_mode : uint8_t { active = 0, paused, eof };

// One queue round may be spent waiting for replies to land; the kind says which
// side owns the round (see drain_input_then_close / finish_peer_closed).
enum class close_round : uint8_t { none = 0, close, peer_closed };

struct conn_state {
    conn_phase  phase = conn_phase::open;
    read_mode   read  = read_mode::active;
    bool        write_armed = false;
    // A drain_input_then_close() serving round is on the stack. Read-side
    // backpressure does not apply inside one: pausing there is what made a
    // close requested while the connection was read-paused serve nothing, park,
    // and FIN over the requests already sitting in rbuf.
    bool        in_round = false;
    close_round round = close_round::none;
    // Input this graceful close still owes the peer: the bytes buffered and
    // unserved when the close was first requested. Negative means "not
    // measured yet".
    int64_t     close_input_bytes = -1;
    int64_t     close_started_at_ms = 0;  // when the close was parked
    int64_t     paused_at_ms = 0;         // when EPOLLIN was dropped, else 0
};

// What the machine needs to know about the connection's buffers to decide.
// Passed in rather than reached for, which is what keeps the transitions pure.
struct conn_facts {
    bool   wbuf_empty = true;
    bool   rbuf_empty = true;
    size_t rbuf_readable = 0;
    size_t pending = 0;     // pending_bytes: reserved, not yet in the kernel
};

// An implied resume of reads, for the reactor to apply (EPOLL_CTL_MOD, the
// frame-assembly deadline shift, the continuation). resumed == false means the
// connection was not paused and there is nothing to apply.
struct read_resume {
    bool    resumed = false;
    int64_t paused_for_ms = 0;
};

struct close_step {
    close_action action = close_action::none;
    read_resume  resume;
};

namespace conn_sm {

inline bool is_closing(const conn_state& s) noexcept {
    return s.phase == conn_phase::closing || s.phase == conn_phase::fin_sent;
}
// Nothing left to park: FIN is out, or the connection is gone.
inline bool is_done(const conn_state& s) noexcept {
    return s.phase == conn_phase::fin_sent || s.phase == conn_phase::closed;
}
// Whether read-side backpressure may act: not once a close is parked, and not
// inside a serving round.
inline bool backpressure_applies(const conn_state& s) noexcept {
    return (s.phase == conn_phase::open || s.phase == conn_phase::draining) &&
           !s.in_round;
}

// The invariant every transition preserves, as the name of the first clause a
// state breaks (nullptr when it breaks none). Anything this names is a
// combination of fields that no sequence of events can produce.
inline const char* violation(const conn_state& s) noexcept {
    const conn_phase ph = s.phase;
    if ((s.read == read_mode::paused) != (s.paused_at_ms != 0))
        return "paused <=> pause timestamp";
    if (s.read == read_mode::paused &&
        ((ph != conn_phase::open && ph != conn_phase::draining) || s.in_round))
        return "paused only in open/draining, outside a serving round";
    if (ph == conn_phase::closed &&
        (s.write_armed || s.in_round || s.read == read_mode::paused))
        return "closed is terminal and normalised";
    if (s.in_round && ph == conn_phase::open)
        return "a serving round needs a requested close";
    if (ph == conn_phase::open && s.close_input_bytes >= 0)
        return "open has no measured input budget";
    if (ph == conn_phase::draining && s.close_input_bytes < 0)
        return "draining has a measured input budget";
    if (is_closing(s) && s.close_started_at_ms == 0)
        return "the close clock runs while parked";
    if ((ph == conn_phase::open || ph == conn_phase::draining) &&
        s.close_started_at_ms != 0)
        return "the close clock is off before parking";
    if (s.round == close_round::close && ph == conn_phase::open)
        return "a close-owned round needs a requested close";
    if (s.round == close_round::peer_closed && s.read != read_mode::eof)
        return "a peer-owned round needs the peer's FIN";
    return nullptr;
}
inline bool valid(const conn_state& s) noexcept {
    return violation(s) == nullptr;
}

// ----- read side -----

// Take EPOLLIN away. False when it may not (already paused, peer FIN seen,
// closed, parked, or inside a serving round): the caller does nothing.
inline bool on_pause(conn_state& s, int64_t now) noexcept {
    if (s.read != read_mode::active || !backpressure_applies(s)) return false;
    s.read = read_mode::paused;
    s.paused_at_ms = now != 0 ? now : 1;
    return true;
}

inline read_resume on_resume(conn_state& s, int64_t now) noexcept {
    read_resume r;
    if (s.read != read_mode::paused) return r;
    s.read = read_mode::active;
    r.resumed = true;
    r.paused_for_ms = s.paused_at_ms != 0 ? now - s.paused_at_ms : 0;
    s.paused_at_ms = 0;
    return r;
}

// recv() returned 0.
inline void on_eof(conn_state& s) noexcept {
    s.read = read_mode::eof;
    s.paused_at_ms = 0;
}

// ----- write side -----

// Target-state, not a toggle. False when nothing changed (or the connection is
// gone, where write interest means nothing).
inline bool on_set_write(conn_state& s, bool armed) noexcept {
    if (s.phase == conn_phase::closed || s.write_armed == armed) return false;
    s.write_armed = armed;
    return true;
}

// The epoll interest set this state asks for. The only place it is derived.
inline uint32_t interest(const conn_state& s) noexcept {
    uint32_t ev = 0;
    if (s.read == read_mode::active) ev |= EPOLLIN;
    if (s.write_armed) ev |= EPOLLOUT;
    return ev;
}

// ----- close protocol -----

// A graceful close that parks now (close_conn graceful, idle timeout).
// Idempotent: once closed or FINed there is nothing left to park. Entering
// closing resumes a paused connection -- the combination is not a state.
inline close_step on_graceful(conn_state& s, int64_t now) noexcept {
    close_step st;
    if (is_done(s)) return st;
    if (s.phase != conn_phase::closing) {
        s.phase = conn_phase::closing;
        s.close_started_at_ms = now != 0 ? now : 1;
        st.resume = on_resume(s, now);
    }
    st.action = close_action::drain_wbuf;
    return st;
}

// A public close (close_connection / stop_gracefully): serve buffered input
// first, so answers to already-arrived requests still reach the peer. Resumes
// a paused connection, because serving the backlog is the whole point. The
// budget is rbuf occupancy at this moment and nothing else -- bytes still in
// the kernel receive buffer are not counted, and the drain's own round is the
// only thing that brings any of them in (see drain_input_then_close for what
// that does and does not promise).
inline close_step on_request(conn_state& s, const conn_facts& f,
                             int64_t now) noexcept {
    close_step st;
    if (is_done(s)) return st;
    if (s.close_input_bytes < 0)
        s.close_input_bytes = static_cast<int64_t>(f.rbuf_readable);
    if (s.phase == conn_phase::open) s.phase = conn_phase::draining;
    st.resume = on_resume(s, now);
    st.action = close_action::drain_input;
    return st;
}

// Bracket a serving round. A no-op outside draining/closing, where a round
// cannot exist.
inline void on_round_begin(conn_state& s) noexcept {
    // requires: reads are not paused -- on_request resumed them.
    assert(s.read != read_mode::paused);
    if (s.phase == conn_phase::draining || s.phase == conn_phase::closing)
        s.in_round = true;
}
inline void on_round_end(conn_state& s) noexcept { s.in_round = false; }

// The park decision shared by on_input_drained and on_input_defer_failed:
// peer_closed owns the round; an already-close round (or a round that served
// nothing) parks now; otherwise defer one park round.
inline close_action park_decision(conn_state& s, int served) noexcept {
    if (s.round == close_round::peer_closed) return close_action::none;
    if (served == 0 || s.round == close_round::close)
        return close_action::drain_wbuf;
    s.round = close_round::close;
    return close_action::defer_park;
}

// A serving round finished. served is the frame count, dealt the bytes this
// round took off the close_input_bytes budget.
inline close_action on_input_drained(conn_state& s, const conn_facts& f,
                                     int served, int64_t dealt) noexcept {
    if (is_done(s)) return close_action::none;
    // requires: a close was requested (on_request measured the budget).
    assert(s.close_input_bytes >= 0);
    // Clamped at zero, not allowed to go negative: negative is the "not
    // measured yet" sentinel, and an exhausted budget that read as unmeasured
    // let a later close request re-measure it and hand a peer that keeps
    // sending a fresh budget -- the very thing the budget exists to prevent.
    if (served > 0 && dealt > 0)
        s.close_input_bytes = std::max<int64_t>(0, s.close_input_bytes - dealt);
    if (served > 0 && !f.rbuf_empty && s.close_input_bytes > 0)
        return close_action::defer_drain_input;
    if (served == 0 && s.round == close_round::none && f.pending > 0) {
        s.round = close_round::close;
        return close_action::defer_park;
    }
    return park_decision(s, served);
}

// Re-decide after the input-defer hand-off failed: drain_input is off the table
// and served was > 0, so what remains is the park decision.
inline close_action on_input_defer_failed(conn_state& s) noexcept {
    // requires: on_input_drained just answered defer_drain_input.
    assert(s.close_input_bytes >= 0);
    return park_decision(s, 1);
}

// What to do after a wbuf drain. dead/error end the connection; stalled leaves
// the rest to a later EPOLLOUT; drained means wbuf is empty and the close
// decision (FIN or otherwise) is made by the caller.
inline close_action on_drain_result(drain_outcome r) noexcept {
    if (r == drain_outcome::error || r == drain_outcome::dead)
        return close_action::close_abort;
    if (r == drain_outcome::drained) return close_action::drain_complete;
    return close_action::none;  // stalled
}

// The peer FINed (read is already eof). A productive wbuf means it is not our
// turn yet: the caller parks, handle_write resumes this once the bytes are out.
// defer_peer_closed spends the one allowed queue round waiting for replies to
// land; otherwise FIN (if we are closing) or tear down.
inline close_action on_peer_fin(conn_state& s, const conn_facts& f) noexcept {
    assert(s.read == read_mode::eof);
    if (s.phase == conn_phase::closed) return close_action::none;
    if (!f.wbuf_empty) return close_action::none;
    if (!is_closing(s) && f.pending > 0 &&
        s.round != close_round::peer_closed) {
        s.round = close_round::peer_closed;
        return close_action::defer_peer_closed;
    }
    if (s.phase == conn_phase::closing) return close_action::send_fin;
    return close_action::close_graceful;
}

// shutdown(SHUT_WR) succeeded.
inline void on_fin_sent(conn_state& s) noexcept {
    assert(s.phase == conn_phase::closing);
    s.phase = conn_phase::fin_sent;
}

// Teardown. True the first time only. Normalises everything a dead connection
// must not claim: no write interest, no round, no pause.
inline bool on_closed(conn_state& s) noexcept {
    if (s.phase == conn_phase::closed) return false;
    s.phase = conn_phase::closed;
    s.write_armed = false;
    s.in_round = false;
    if (s.read == read_mode::paused) {
        s.read = read_mode::active;
        s.paused_at_ms = 0;
    }
    return true;
}

}  // namespace conn_sm

// The machine a connection owns: the pure state above, its published phase,
// the cross-thread request channel, and a read-only view of the buffers the
// decisions are taken against (set once by attach()).
class conn_machine {
 public:
    conn_machine() noexcept = default;
    conn_machine(const conn_machine&) = delete;
    conn_machine& operator=(const conn_machine&) = delete;

    void attach(ring_buffer* wbuf, ring_buffer* rbuf,
                std::atomic<size_t>* pending_bytes) noexcept {
        wbuf_ = wbuf;
        rbuf_ = rbuf;
        pending_bytes_ = pending_bytes;
    }

    // ---- readable from any thread ----

    conn_phase published_phase() const noexcept {
        return static_cast<conn_phase>(pub_.load(std::memory_order_acquire));
    }
    // A graceful close has been parked: submissions are refused from here on,
    // but the peer keeps reading what we owe it.
    bool closing() const noexcept {
        const conn_phase p = published_phase();
        return p == conn_phase::closing || p == conn_phase::fin_sent;
    }
    bool closed() const noexcept {
        return published_phase() == conn_phase::closed;
    }
    // closing or closed: what send_to, the queued send and the owner-thread
    // fast path all ask, in one load, so they cannot disagree about it.
    bool refuses_sends() const noexcept {
        return published_phase() >= conn_phase::closing;
    }

    // ---- cross-thread request channel ----
    //
    // Close requests this connection has accepted. Non-zero for the rest of its
    // life once one is queued. The owner-thread send_to() fast path falls back
    // to the queued path while this is non-zero, so a send issued after
    // close_connection() keeps the existing queue order (and its existing drop)
    // instead of overtaking it. Counted before the queue attempt, not after it:
    // a handler already running on the owner thread could otherwise pass the
    // fast-path check in that window and write ahead of a close that was
    // already queued (R9-4). The count is never given back: a request the
    // queue refused is still honoured (see post_close_wanted), so the
    // connection is closing from then on either way.
    void note_close_request() noexcept {
        close_requests_.fetch_add(1, std::memory_order_release);
    }
    bool close_request_pending() const noexcept {
        return close_requests_.load(std::memory_order_acquire) != 0;
    }
    // A close the queue refused. The tick sweep honours it, so a close request
    // is never lost to a full queue.
    void post_close_wanted() noexcept {
        close_wanted_.store(true, std::memory_order_release);
    }
    // Consumes the flag; true if it was set.
    bool take_close_wanted() noexcept {
        return close_wanted_.load(std::memory_order_acquire) &&
               close_wanted_.exchange(false, std::memory_order_acq_rel);
    }

    // ---- reactor thread only ----

    const conn_state& state() const noexcept { return st_; }
    bool fin_sent() const noexcept { return st_.phase == conn_phase::fin_sent; }
    bool read_paused() const noexcept { return st_.read == read_mode::paused; }
    // The peer's FIN has been seen (recv returned 0). Set once, in handle_read;
    // handle_write consults it to finish a parked close deterministically the
    // moment the drain completes, instead of waiting for a spurious EPOLLIN.
    bool peer_closed() const noexcept { return st_.read == read_mode::eof; }
    bool write_armed() const noexcept { return st_.write_armed; }
    bool backpressure_applies() const noexcept {
        return conn_sm::backpressure_applies(st_);
    }
    int64_t close_started_at_ms() const noexcept {
        return st_.close_started_at_ms;
    }
    bool wbuf_empty() const noexcept { return !wbuf_ || wbuf_->empty(); }
    uint32_t interest() const noexcept { return conn_sm::interest(st_); }

    // ---- events ----

    bool on_pause(int64_t now) noexcept {
        const bool r = conn_sm::on_pause(st_, now);
        commit();
        return r;
    }
    read_resume on_resume(int64_t now) noexcept {
        const read_resume r = conn_sm::on_resume(st_, now);
        commit();
        return r;
    }
    void on_eof() noexcept { conn_sm::on_eof(st_); commit(); }
    bool on_set_write(bool armed) noexcept {
        const bool r = conn_sm::on_set_write(st_, armed);
        commit();
        return r;
    }

    close_step on_graceful(int64_t now) noexcept {
        const close_step r = conn_sm::on_graceful(st_, now);
        commit();
        return r;
    }
    close_step on_request(int64_t now) noexcept {
        const close_step r = conn_sm::on_request(st_, facts(), now);
        commit();
        return r;
    }
    void on_round_begin() noexcept { conn_sm::on_round_begin(st_); commit(); }
    void on_round_end() noexcept { conn_sm::on_round_end(st_); commit(); }
    close_action on_input_drained(int served, int64_t dealt) noexcept {
        const close_action a =
            conn_sm::on_input_drained(st_, facts(), served, dealt);
        commit();
        return a;
    }
    close_action on_input_defer_failed() noexcept {
        const close_action a = conn_sm::on_input_defer_failed(st_);
        commit();
        return a;
    }
    close_action on_peer_fin() noexcept {
        const close_action a = conn_sm::on_peer_fin(st_, facts());
        commit();
        return a;
    }
    void on_fin_sent() noexcept { conn_sm::on_fin_sent(st_); commit(); }
    // True the first time; the arbiter that makes teardown idempotent.
    bool on_closed() noexcept {
        const bool r = conn_sm::on_closed(st_);
        commit();
        return r;
    }
    // A connection the reactor refused before it ever served: same terminal
    // transition, named for what the caller means.
    void on_rejected() noexcept { on_closed(); }

 private:
    conn_facts facts() const noexcept {
        conn_facts f;
        f.wbuf_empty = !wbuf_ || wbuf_->empty();
        f.rbuf_empty = !rbuf_ || rbuf_->empty();
        f.rbuf_readable = rbuf_ ? rbuf_->readable() : 0;
        f.pending = pending_bytes_
                        ? pending_bytes_->load(std::memory_order_relaxed) : 0;
        return f;
    }
    // Every transition ends here: the invariant holds, and the phase other
    // threads read is the one the reactor now has.
    void commit() noexcept {
#ifndef NDEBUG
        if (const char* why = conn_sm::violation(st_)) {
            std::fprintf(stderr,
                "[fatal] conn_machine invariant broken: %s "
                "(phase=%d read=%d write_armed=%d in_round=%d round=%d "
                "close_input_bytes=%" PRId64 " close_started_at_ms=%" PRId64
                " paused_at_ms=%" PRId64 ")\n",
                why, static_cast<int>(st_.phase), static_cast<int>(st_.read),
                static_cast<int>(st_.write_armed),
                static_cast<int>(st_.in_round), static_cast<int>(st_.round),
                static_cast<int64_t>(st_.close_input_bytes),
                static_cast<int64_t>(st_.close_started_at_ms),
                static_cast<int64_t>(st_.paused_at_ms));
            std::abort();
        }
#endif
        pub_.store(static_cast<uint8_t>(st_.phase), std::memory_order_release);
    }

    conn_state              st_;
    std::atomic<uint8_t>    pub_{static_cast<uint8_t>(conn_phase::open)};
    std::atomic<uint32_t>   close_requests_{0};
    std::atomic<bool>       close_wanted_{false};
    ring_buffer*            wbuf_ = nullptr;
    ring_buffer*            rbuf_ = nullptr;
    std::atomic<size_t>*    pending_bytes_ = nullptr;
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
    // Consequence: a peer that sends bytes which never complete a frame is not
    // idle-reclaimed. It is bounded by frame_assembly_timeout_ms instead (see
    // do_tick), by rbuf (a frame larger than rbuf_capacity is closed as
    // incomplete), and by TCP keepalive.
    int64_t    last_recv_ms = 0;
    // Timestamp of the first byte of the frame being assembled; 0 means no
    // frame is in progress. Set for both halves: a header that has not fully
    // arrived, and a payload that has not finished arriving. Later bytes do not
    // refresh it, so the deadline derived from it (this field plus
    // frame_assembly_timeout_ms, see do_tick) measures how long the frame has
    // been incomplete, not how long the peer has been quiet. Paused intervals
    // are added back on resume (see conn_machine::on_resume), so it
    // measures the time the
    // peer had to complete the frame, not the time the server spent not
    // reading it.
    int64_t    frame_started_at_ms = 0;
    // Inbound bytes that have arrived but not been parsed: a partial frame plus
    // whatever complete frames came in the same read. Reactor-thread-only.
    ring_buffer rbuf;
    // Outbound bytes, whole frames only. Nothing is appended until a frame is
    // complete, which is what lets send_frame_remain below name a single frame.
    ring_buffer wbuf;
    // Last time the drain moved bytes out of wbuf. The idle sweep uses it
    // together with last_recv_ms: while paused there may be no inbound bytes,
    // but a peer that is still reading is not idle.
    int64_t    last_drain_progress_ms = 0;
    // Bytes of the frame at the head of wbuf that have not reached the kernel.
    // wbuf only ever receives whole frames, so the head is either inside
    // exactly one frame or exactly at a frame boundary; this field says which,
    // so the drain can count a frame when its last byte leaves instead of when
    // it is queued. Reactor-thread-only, like wbuf itself.
    size_t      send_frame_remain = 0;
    // -------- accounting for the close protocol --------
    // Everything that decides what the connection may do next -- phase,
    // read pause, peer FIN, write interest, the close rounds -- lives in `sm`.
    // What is kept here is accounting, not state:
    // Bytes this connection's handle_read() has taken off the socket since
    // drain_input_then_close() zeroed it, which is always either nobody or the
    // graceful close: it is how a round can be charged for input that arrived
    // *during* the round. Reactor-thread-only, like close_input_bytes.
    size_t     drain_read_bytes = 0;
    // The connection's one state machine (see conn_machine).
    conn_machine sm;

    // Shared across threads from here down:
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
    friend class multi_reactor_server;
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
    // Returns the number of frames this call parsed and served (PING and PONG
    // included, since a PING produces a PONG). Zero means the read produced no
    // work at all, which drain_input_then_close() uses to tell "the peer asked
    // for nothing more" from "answers are queued behind us".
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
    // The one place that applies the connection's epoll interest, which the
    // state machine derives (conn_sm::interest) from its read mode and write
    // flag.
    void apply_interest(connection* c, const char* fail_reason) noexcept;
    void pause_read(connection* c) noexcept;
    void resume_read(connection* c) noexcept;
    void apply_resume(connection* c, const read_resume& r) noexcept;
    void maybe_resume_read(connection* c) noexcept;
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
    // Tears down every connection this reactor still owns. reason names the
    // cause in the close callback; a quarantined reactor passes
    // "reactor failure", a normal shutdown keeps "server shutdown".
    void cleanup(const char* reason = "server shutdown") noexcept;
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
    // Set once when this reactor's loop died (a throw, or epoll_wait failing).
    // It is the quarantine flag: pick_reactor() skips it, the reactor's own
    // connections are reset by cleanup(), and the accept thread joins it
    // through stop(). Read with acquire so the slot/counters published before
    // the store are visible to whoever observes the quarantine.
    std::atomic<bool> dead_{false};
    // Set once by the accept thread's reaper immediately before it calls
    // stop() on a dead reactor, so the join happens exactly once. CAS'd rather
    // than stored: two reaping passes must not both enter stop().
    std::atomic<bool> reaped_{false};
    // The server's fixed counter slot for this reactor, set on start. The
    // array is sized to the maximum reactor count, so idx_ is always in range.
    reactor_slot*     slot_ = nullptr;
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
    // Slots held back for close/drain/park/new-connection tasks, so a flood of
    // application replies cannot refuse the tasks that make progress. Data
    // tasks stop this many short of pending_task_cap_; control tasks may use
    // the whole cap, which bounds the reserve by construction.
    static constexpr size_t k_control_task_reserve = 64;
    static_assert(k_min_pending_tasks > k_control_task_reserve,
                  "the reserve must leave a data-task slot");
    size_t pending_task_cap_ = k_min_pending_tasks;
    size_t data_task_cap_ = k_min_pending_tasks - k_control_task_reserve;
    std::mutex              pending_mutex_;
    std::vector<queue_task> pending_tasks_;
    std::vector<queue_task> active_tasks_;

    // Deferred close notifications, one entry per closed connection. The four
    // fields used to be four parallel vectors "cleared together so their
    // indices always agree" -- which they did not, under the only condition
    // that can fail here: inserting four things can succeed three times. An
    // entry is now one object, so a partial notification is not representable.
    std::vector<closed_conn> pending_closes_;

    std::unordered_map<uint64_t, std::shared_ptr<connection>> conns_;

    std::vector<uint8_t> frame_scratch_;
};

// Defined after the class: GCC will not instantiate a member-function template
// from an in-class initializer. Inline so every translation unit that includes
// the header shares one definition.
inline constexpr sub_reactor::dispatch_table sub_reactor::k_task_dispatch = (
    sub_reactor::make_task_dispatch(std::make_index_sequence<
        std::variant_size_v<sub_reactor::queue_task>>{}));

// Builds the connection's display name ("host:service") and the key the per-IP
// cap uses. A dual-stack listener reports IPv4 peers as ::ffff:a.b.c.d, which
// is stripped so spelling cannot bypass the cap. IPv6 is keyed by /64: the low
// 64 bits are the peer's to choose, and a full-address key hands anyone with
// one /64 a fresh slot per connection. A link-local scope suffix ("%eth0")
// names an interface, not an address, so it is cut before parsing. getnameinfo
// and allocation failure both leave the "unknown" keys, which the cap then
// treats as one peer.
// Not noexcept on purpose: if resetting the keys after a failed assignment
// itself throws, admit_accept's catch must see it and reject the connection
// rather than have std::terminate take every connection with it.
inline void peer_strings_from_sockaddr(const struct sockaddr_storage& ss,
                                       socklen_t slen, std::string* peer,
                                       std::string* ip_key) {
    char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
    *peer = "unknown";
    *ip_key = "unknown";
    if (::getnameinfo(reinterpret_cast<const struct sockaddr*>(&ss), slen,
                      hbuf, sizeof(hbuf), sbuf, sizeof(sbuf),
                      NI_NUMERICHOST | NI_NUMERICSERV) != 0)
        return;
    try {
        ip_key->assign(hbuf);
        static constexpr char k_mapped[] = "::ffff:";
        if (ip_key->size() > sizeof(k_mapped) - 1 &&
            ip_key->compare(0, sizeof(k_mapped) - 1, k_mapped) == 0 &&
            ip_key->find('.', sizeof(k_mapped) - 1) != std::string::npos) {
            ip_key->erase(0, sizeof(k_mapped) - 1);
        }
        if (ip_key->find(':') != std::string::npos) {
            char v6[INET6_ADDRSTRLEN];
            const std::string::size_type pct = ip_key->find('%');
            const std::string::size_type v6len =
                pct == std::string::npos ? ip_key->size() : pct;
            struct in6_addr a6{};
            if (v6len < sizeof v6) {
                std::memcpy(v6, ip_key->data(), v6len);
                v6[v6len] = '\0';
                if (::inet_pton(AF_INET6, v6, &a6) == 1) {
                    std::memset(a6.s6_addr + 8, 0, 8);
                    char masked[INET6_ADDRSTRLEN];
                    if (::inet_ntop(AF_INET6, &a6, masked, sizeof masked) !=
                        nullptr)
                        ip_key->assign(masked);
                }
            }
        }
        peer->assign(hbuf);
        peer->push_back(':');
        peer->append(sbuf);
    } catch (...) {
        *peer = "unknown";
        *ip_key = "unknown";
    }
}

// ==================== multi_reactor_server ====================
class multi_reactor_server {
 public:
    // Install handlers before start; never replace them while running. The
    // installers now enforce that: each returns false and leaves the handler
    // untouched unless the lifecycle is fully stopped, because the workers read
    // these std::functions without synchronization and a store underneath them
    // is a data race rather than a reconfiguration.
    //
    // Handlers run concurrently across workers and must not throw or destroy
    // this server. A throw from a message handler or from a conn-state handler,
    // including the close-side transitions, is caught and dropped, because
    // nothing is left to undo by then and the exception already belongs to the
    // application. Close-side throws are counted in callback_exceptions; before
    // that they reached std::terminate because the flush is noexcept.
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
        // Where to listen. The default accepts on every interface. Nothing here
        // authenticates or encrypts: there is no TLS, and the only admission
        // controls are max_connections, max_total_buffer_bytes and
        // max_connections_per_ip (the last one off unless configured). Whatever
        // can route to this port is a client.
        std::string bind_addr        = "0.0.0.0";
        // 0 means "kernel, pick one" -- and then port() is the only way to
        // learn what was picked, so start() succeeding says nothing about
        // whether anyone can reach the server until port() is read.
        uint16_t    port            = 0;
        // Must be > 0 (start() refuses otherwise): Linux clamps listen()'s
        // backlog only from above, so 0 would pass straight through as a
        // zero-length accept queue -- connections refused under load, silently.
        int         backlog         = 1024;
        // Opt-in SO_REUSEPORT. Off by default: with it on, a second process can
        // bind the same port without error and the kernel silently splits
        // connections between them, so a stale instance is no longer visible as
        // a failed bind. Multi-process deployments (one server per process,
        // same port) turn it on deliberately.
        bool        reuse_port      = false;
        // 0 means one per hardware thread; resolved_reactor_count() clamps to
        // [1, k_max_reactors], and every consumer reads that rather than this.
        int         num_reactors     = 0;
        // Admission ceiling. A connection arriving when this many are already
        // admitted is accepted and closed at once, counted in rejected. Must be
        // > 0. The memory this implies is bounded separately by
        // max_total_buffer_bytes below, whose default is exactly
        // max_connections * (rbuf_capacity + wbuf_capacity): lowering that one
        // caps bytes instead of connections.
        int         max_connections  = 10000;
        // Ceiling on the buffer bytes admitted connections may reserve. 0 means
        // max_connections * (rbuf_capacity + wbuf_capacity) -- the bill the
        // connection count already implies. Set it lower to bound memory
        // independently of the count; start() refuses a value smaller than one
        // connection's buffers and warns when it admits fewer connections.
        size_t      max_total_buffer_bytes = 0;
        // Most live connections one numeric peer address may hold. 0 (default)
        // is unlimited. A peer over the cap is accepted and closed at once,
        // counted in rejected and ip_rejections.
        int         max_connections_per_ip = 0;
        // Close a connection after this long with no activity. Activity is
        // bytes received *or* bytes drained to the peer: a connection paused by
        // read backpressure is not receiving by design, and a client that reads
        // steadily is not idle just because it is slow. Measured in bytes, not
        // frames, so a peer dribbling a partial frame is not idle either;
        // frame_assembly_timeout_ms is what bounds that. Must be > 0.
        int         idle_timeout_ms   = 120000;
        // How often each reactor runs its deadline scan. Every timeout here
        // fires on one of these boundaries, so this is the granularity they all
        // share. Must be > 0; start() warns when it is coarser than a deadline
        // it governs.
        int         tick_ms          = 500;
        // Watchdog for a reactor that is alive but not making progress (for
        // example a handler that never returns). 0 disables it. Otherwise it
        // must be at least max(100, 2 * tick_ms): a healthy reactor stamps its
        // progress once per iteration, and an iteration is one tick plus the
        // loop body, so a small margin over tick_ms reports healthy reactors.
        // The timeout must still exceed the longest expected iteration -- a
        // large connection set or a slow inline handler stretches the body.
        // The accept thread samples at least every min(500 ms, timeout / 2),
        // so a stall shorter than timeout plus one sample can fall between
        // samples. Two effects, both on the accept side: the episode is counted
        // and logged once (reactor_stalls), and the reactor stops being handed
        // new connections (reactor_stall_skips) until it has made progress for
        // a full timeout window again. Its existing connections keep being
        // attempted by it, and nothing is torn down.
        int         reactor_stall_timeout_ms = 0;
        // A frame must complete within this deadline once its first byte
        // arrives; 0 means 5 x idle_timeout_ms. Unlike idle_timeout_ms this
        // measures progress, so a peer that keeps dribbling a partial frame
        // cannot hold the connection forever. Must be >= 0 (start() refuses
        // negative).
        int         frame_assembly_timeout_ms = 0;
        // Whether a frame whose type is neither DATA (1), PING (2) nor PONG (3)
        // is handed to the message handler. Default false: those three values
        // are the whole protocol, so any other one says the peer speaks a
        // different protocol, not that the stream is corrupt. The connection is
        // closed with "unknown frame type" and counted in unknown_frame_types
        // rather than parse_errors, because the two have different remedies.
        // The type is in the header, so the close happens as soon as those
        // eight bytes arrive rather than after the declared payload does.
        //
        // True restores the older behaviour, where the type space was shared
        // with the application and every other value was delivered as DATA.
        // That is a trust decision the application takes on explicitly: nothing
        // authenticates the peer in front of this entry point, so an unknown
        // type reaching the handler is untrusted input selecting an application
        // code path. Applications that need their own opcodes are better served
        // by putting them in the DATA payload, which keeps the type space a
        // closed set the library can validate.
        bool        allow_unknown_frame_types = false;
        // Per-connection read buffer, allocated on accept. Must be >=
        // MAX_FRAME_SIZE (start() refuses otherwise): a frame that does not fit
        // is closed as incomplete, so a smaller buffer makes a legal maximum
        // frame undeliverable. The floor is 65544, not the 65536 that "64 KiB"
        // suggests -- 8 bytes of header sit outside MAX_FRAME_PAYLOAD.
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
        // Cap on the kernel send buffer of each accepted socket. 0 leaves the
        // OS default. A smaller buffer makes read backpressure engage sooner
        // and bounds the kernel memory one connection can pin; the kernel
        // enforces its own minimum, so a tiny value is clamped, not refused.
        size_t      socket_send_buffer_bytes = 0;
        // Read-side backpressure. Above the high mark the reactor stops
        // reading (and drops EPOLLIN) until pending output falls to the low
        // mark; this is what lets a slow-but-alive client be slowed down
        // instead of reset when its replies back up. 0 means "decide for me":
        // high = 3/4 of the effective max_pending_bytes, low = high / 2.
        // Explicit values must satisfy 0 < low < high <= max_pending_bytes;
        // start() validates them after the max_pending_bytes clamp.
        size_t      read_high_watermark_bytes = 0;
        size_t      read_low_watermark_bytes  = 0;
        // Most bytes one connection may hand to the kernel in a single drain
        // before drain_wbuf_to_socket() returns and the reactor moves on, so
        // one fast sender cannot monopolise the loop. Despite the name it is
        // not a per-iteration budget: every readable connection gets its own.
        // Must be > 0.
        size_t      write_budget_bytes = 256 * 1024;
        // Total budget for a graceful close, counted from the moment it is
        // parked: drain wbuf, send the FIN, then wait for the peer's FIN. It
        // bounds the descriptor cost of a peer that ACKs the FIN and never
        // answers it, and of one that never lets the drain finish. Must be > 0
        // (start() refuses otherwise): with no deadline a parked close leaks
        // the fd and its admission slot forever.
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

        // Read watermarks, with "0 means decide for me" resolved against the
        // *effective* max_pending_bytes (start() clamps it to wbuf_capacity).
        // Derived high leaves a quarter of the ceiling unused so a PONG can
        // still be admitted just below it.
        size_t resolved_read_high_watermark_bytes() const noexcept {
            if (read_high_watermark_bytes > 0) return read_high_watermark_bytes;
            const size_t maxp = max_pending_bytes;
            size_t high = maxp - maxp / 4;
            if (high == 0) high = maxp;
            return high;
        }
        size_t resolved_read_low_watermark_bytes() const noexcept {
            if (read_low_watermark_bytes > 0) return read_low_watermark_bytes;
            const size_t high = resolved_read_high_watermark_bytes();
            const size_t low = high / 2;
            return low > 0 ? low : high;
        }

        // Buffer bytes one connection reserves, and the global ceiling they
        // are admitted against. The product saturates instead of wrapping: a
        // wrapped budget would be a small number that rejects everything.
        size_t per_connection_buffer_bytes() const noexcept {
            return rbuf_capacity + wbuf_capacity;
        }
        size_t resolved_max_total_buffer_bytes() const noexcept {
            if (max_total_buffer_bytes > 0) return max_total_buffer_bytes;
            const size_t per = per_connection_buffer_bytes();
            if (per == 0) return 0;
            const size_t cap = static_cast<size_t>(max_connections);
            if (cap > static_cast<size_t>(-1) / per)
                return static_cast<size_t>(-1);
            return cap * per;
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
    // about it. stop() below declines to join the calling thread, since a
    // thread cannot join itself, so accept_thread_ is still joinable when this
    // body returns and std::thread::~thread calls std::terminate(). The default
    // message, "terminate called without an active exception", names neither
    // this server nor the reason, so a reader cannot tell what died. Say it
    // here, then let the terminate happen: there is no correct recovery. The
    // fix is for the last owner to be released after the controlling thread's
    // stop() has joined, which is what the contract above asks for.
    static constexpr const char* k_fatal_error =
        "[fatal] multi_reactor_server destroyed from inside its own "
        "callback.\n"
        "  stop() cannot join the calling thread, so the accept/worker "
        "threads\n"
        "  are still joinable and std::thread::~thread is about to "
        "terminate.\n"
        "  Call stop() from the controlling thread, and release the last "
        "owner\n"
        "  only after it has returned.\n";
    ~multi_reactor_server() noexcept {
        if (callback_server_ == this) {
            std::fprintf(stderr, k_fatal_error);
            std::fflush(stderr);
        }
        stop();
    }

    multi_reactor_server(const multi_reactor_server&) = delete;
    multi_reactor_server& operator=(const multi_reactor_server&) = delete;

    // One call per DATA frame, in arrival order per connection. A throw is
    // caught and dropped (see the contract above). False means the server was
    // not fully stopped, so the handler was left as it was and the refusal is
    // counted in handler_installs_refused -- the install is silent otherwise,
    // and "my new handler never runs" is a long way from the cause.
    bool set_message_handler(message_handler h) noexcept {
        return install_handler([this, &h]() noexcept {
            msg_handler_ = std::move(h);
        });
    }
    // open==true once the connection is admitted, false when it is gone. The
    // close side runs on the deferred path; a throw from either call is caught
    // and counted in callback_exceptions, so one bad callback cannot take the
    // process down with it. Same install rule and return value as above.
    bool set_conn_state_handler(conn_state_handler h) noexcept {
        return install_handler([this, &h]() noexcept {
            state_handler_ = std::move(h);
        });
    }
    // reason is borrowed and valid for the callback; includes shutdown
    // closures. Same install rule and return value as above.
    bool set_close_handler(close_handler h) noexcept {
        return install_handler([this, &h]() noexcept {
            close_handler_ = std::move(h);
        });
    }

    // Binds, listens and starts the accept thread and the reactors. False --
    // with no thread created and the lifecycle left stopped -- when called from
    // a callback, when the server is not fully stopped, or when the
    // configuration is refused; the bounds are on the fields above, and every
    // one of them is checked here rather than at first use.
    bool start() noexcept;
    // Abortive and idempotent. Stops accepting, closes the listening socket and
    // tears every connection down without draining it: queued output is
    // discarded and the peer sees RST rather than FIN. Joins the accept thread
    // and every reactor, so it belongs on the controlling thread; called from a
    // callback it only requests a stop and returns, leaving the join to a later
    // call. Queued offload tasks are dropped, not run.
    void stop()  noexcept;
    // Stop accepting, ask every open connection to close gracefully, and wait
    // up to timeout_ms for them to disappear before the normal (abortive) stop.
    // true means the connection table and the admission count are both empty --
    // which a connection aborted at the end of close_linger_ms satisfies just
    // as well, so true does not promise that nothing was reset; stats().aborted
    // is the counter that separates the two. false means the deadline was hit
    // with something still open, or that this was called from a callback, where
    // it only requests a stop and returns. Call from the controlling thread.
    bool stop_gracefully(int timeout_ms) noexcept;
    // Asks the running server to wind down and returns without joining
    // anything, which is what makes it the one lifecycle call a callback may
    // use. The reactors see the state at their next tick, and the controlling
    // thread still has to call stop() to join the threads and close the fds.
    // Not safe from a signal handler: it takes accept_fd_mutex_ and writes the
    // wake eventfd.
    void request_stop() noexcept { request_stop_internal(); }
    // Blocks while is_running() holds, polling every 200ms. That is not the
    // same as "until stop() was called": the accept thread's failure, or the
    // failure of the last live reactor, clears the same flag, so returning
    // means the server is down, not that it was asked to stop. A reactor that
    // fails while another is alive is quarantined and does not make this
    // return. Never call it from a callback -- that thread would be waiting
    // for a condition only its own exit can satisfy, while stop() waits to join
    // it.
    void wait()  noexcept;

    // ok means accepted (written straight to wbuf on the owner thread, or
    // queued otherwise), not delivered. Shutdown or disconnection can discard
    // work.
    send_result send_to(uint64_t conn_id, const void* data,
                        size_t len) noexcept;
    // True means the close has been accepted: either queued to the owning
    // reactor, or -- when that queue was at its cap -- recorded in close_wanted
    // for do_tick's sweep, which performs the same drain-then-close within one
    // tick. False means there was nothing to close: no such connection, no
    // owner to hand it to, or the server is not running. A refused queue
    // hand-off still counts in close_requests_dropped even though it returns
    // true, because that counter is what tells an operator the reactor queues
    // are under pressure.
    bool       close_connection(uint64_t conn_id) noexcept;

    // Runs fn on a handler-pool worker; tasks submitted for one connection run
    // in order. Returns false when no pool is configured/running or its queue
    // is full. A message handler can copy its payload and call this, then
    // return; the reactor is not blocked. fn runs off the reactor thread, so
    // send_to from it uses the queued path. True means queued, not "will run":
    // stop() discards what is still waiting, and a connection closing does not
    // cancel an accepted task -- only its later send_to fails.
    bool offload(uint64_t conn_id, std::function<void()> fn) noexcept;

    size_t connection_count() const noexcept;
    // Snapshot of every live connection id. It allocates once per shard, so
    // bad_alloc reaches the caller (reactor_stats() also allocates its result
    // vector and can throw for the same reason). stop_gracefully() calls it
    // from a noexcept function on a 10ms loop, and catches the exception there
    // so a bad_alloc falls through to stop() and a returned false rather than
    // terminating the process.
    std::vector<uint64_t> connection_ids() const;

    // Where the listening socket actually is. Equals config::port unless that
    // was 0, in which case this is the ephemeral port the kernel chose -- the
    // only place that number exists after bind(). 0 before start() has bound
    // and after stop() has closed: no socket, no port, and a stopped server
    // must not keep reporting one. Read it after start(), not before.
    uint16_t port() const noexcept;

    // Live counters, not a snapshot: the reference stays valid until the server
    // is destroyed, and values keep moving while they are read. Each field is
    // atomic on its own; a group of them read together is not a consistent
    // picture, and the four traffic counters are published once per loop
    // iteration, so they can trail by an iteration under load.
    const server_stats& stats() const noexcept { return stats_; }

    // Per-reactor snapshot. The vector has one entry per slot used since
    // construction, in reactor-index order, and is empty before the first
    // start allocates the slots. An entry is current while its index is below
    // the reactor count of the latest successful start; surplus slots from an
    // earlier, larger run stay in the vector with `retired` set. A retired
    // slot reports no per-run state but keeps its cumulative traffic, so the
    // four totals still add up to server_stats(). After a start that failed
    // after allocation every slot is retired. The traffic counters are
    // cumulative since construction; `connections` is the count the reactor
    // last published (0 after its teardown); `last_progress_ms` is the
    // steady-clock millisecond of its last completed loop iteration (0 until
    // it runs); `dead` is set when its loop failed and the reactor was
    // quarantined -- that slot takes no new connections and its connections
    // are reset; and `stalled` is set while the watchdog sees no progress,
    // which stops new connections from being routed here but tears nothing
    // down. Reads are per-field atomics from a fixed slot array, so a
    // snapshot can mix two adjacent iterations; it is not a transactional
    // view. The result is a vector, so this call can throw bad_alloc.
    struct reactor_stats_view {
        uint64_t frames_recv = 0;
        uint64_t frames_sent = 0;
        uint64_t bytes_recv  = 0;
        uint64_t bytes_sent  = 0;
        size_t   connections = 0;
        int64_t  last_progress_ms = 0;
        bool     dead = false;
        // True while the watchdog considers this reactor stalled: no new
        // connections are routed to it. Always false when the watchdog is off;
        // quarantine clears it when `dead` is set, because a dead slot is not
        // sampled again.
        bool     stalled = false;
        // True when this slot is left over from a larger earlier run. Its
        // per-run fields are zero; only its cumulative traffic is meaningful.
        bool     retired = false;
    };
    std::vector<reactor_stats_view> reactor_stats() const;

    // sub_reactor internal interface
    bool   on_connection_opened(uint64_t id,
                                std::shared_ptr<connection> c) noexcept;
    void   on_connection_closed(uint64_t id) noexcept;
    // running means: lifecycle reached running AND no fatal failure has been
    // raised. A reactor failure is not fatal while another reactor is alive:
    // the dead reactor is quarantined (no new connections, its connections
    // reset) and true keeps being returned. The last live reactor's failure,
    // or the accept thread's, raises failed_ and makes this false. Neither
    // rewrites the lifecycle state, so start()'s success/failure verdict stays
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

    // Shared body of the three handler installers. The lifecycle mutex is
    // taken with try_to_lock on purpose: a setter that races start() or stop()
    // is refused rather than queued behind it. Waiting would deadlock the one
    // caller that is already forbidden but still possible -- a setter invoked
    // from a callback while the controlling thread holds this mutex inside
    // stop() and waits to join that very thread. Refusing is also the honest
    // answer: the caller wanted the handler in place before the workers
    // existed, and that moment has passed.
    //
    // Holding the same mutex start() takes is what makes the check more than
    // advisory. A plain read of life_ followed by an assignment can interleave
    // with start()'s first store; a setter that could do that would be the race
    // this exists to prevent.
    template <typename Install>
    bool install_handler(Install&& install) noexcept {
        std::unique_lock<std::mutex> lk(lifecycle_mutex_, std::try_to_lock);
        if (!lk.owns_lock() ||
            life_.load(std::memory_order_acquire) != life::stopped) {
            stats_.handler_installs_refused.fetch_add(
                1, std::memory_order_relaxed);
            return false;
        }
        try {
            install();
        } catch (...) {
            // Reachable only if a future install body throws: the std::function
            // move assignments this is used for are noexcept. Count it with the
            // same counter as a refusal, because the caller's question is the
            // same ("did my handler go in?"), and answer it the same way.
            stats_.handler_installs_refused.fetch_add(
                1, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    // ---- test seam ---------------------------------------------------------
    // The fault switches are present in every build and checked in every
    // build, because a class whose definition varies with a macro is an ODR
    // hazard: two translation units that disagree about this class have no
    // defined behaviour, and the failure shows up as a missing member at link
    // time or as a wrong offset at run time, far from the build flag that
    // caused it.
    //
    // That is the argument for always present, not for public. They are
    // private, and reachable only through `test_access`, which this class
    // befriends and which the test support header defines. Nothing that merely
    // holds a server reference can fire one -- which used to be the whole
    // hazard: a stray line anywhere in the process could kill a reactor, and
    // in production no less.
    friend struct test_access;
    // One-shot: the next loop iteration of exactly one reactor throws. The
    // switch is consumed with a load followed by an exchange, so two reactors
    // cannot both observe true (a load/store pair let every reactor scheduled
    // in the same window throw, which made "one deterministic death"
    // untestable). The loop reads it with a relaxed load and only pays for the
    // exchange when it is set.
    std::atomic<bool> fail_worker_for_test_{false};
    std::atomic<bool> fail_accept_for_test_{false};
    // One-shot, -1 = off. Otherwise the reactor whose index matches throws on
    // its next loop iteration; the switch resets itself to -1 when it fires.
    // The fault-injection suite uses it to quarantine a chosen reactor -- in
    // particular the one owning a connection, found through
    // reactor_stats()[i].connections.
    std::atomic<int> fail_worker_reactor_for_test_{-1};
    // One-shot: the next accept setup's first epoll registration fails, after
    // the spare descriptor has been opened. Used to prove a failed start
    // releases the spare as well as the descriptors start() knows about.
    std::atomic<bool> fail_epoll_add_for_test_{false};
    // One-shot: the next handler-pool start refuses, after the listening
    // socket, the reactors and the accept thread exist. Used to prove the
    // deepest start rollback joins and releases all of them. Checked before
    // the no-pool early return, so it fires on any configuration.
    std::atomic<bool> fail_handler_pool_for_test_{false};
    // -1 = off. Otherwise the Nth reactor start (zero-based) refuses, after
    // N reactors have already started, which exercises the partial-set
    // teardown. One-shot: reset to -1 when it fires.
    std::atomic<int> fail_reactor_start_after_for_test_{-1};
    // One-shot: the next submit_close() reports a full queue. Used to prove
    // that a refused close request still reaches the connection through the
    // tick sweep.
    std::atomic<bool> fail_close_enqueue_for_test_{false};
    // -1 = off. Otherwise the next start() uses this many reactors instead of
    // config::resolved_reactor_count(), clamped to [1, k_max_reactors]. Lets
    // the fault-injection suite shrink or grow the pool between runs without
    // depending on hardware_concurrency() changing. Persistent until reset,
    // because a test needs two consecutive starts to disagree.
    std::atomic<int> resolved_reactor_count_for_test_{-1};

    // Lifecycle states. Only the thread holding lifecycle_mutex_ moves the
    // state out of/into stopped; threads that merely observe a fatal error use
    // fail().
    enum class life : int {
        stopped = 0, starting = 1, running = 2, stopping = 3
    };

    // Single upward-only failure flag, raised by the accept loop and by the
    // last live reactor. A reactor failing while another is alive does not
    // touch it: that reactor is quarantined, not a server failure.
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

    // start()'s phases. validate_start_config() resolves and checks the
    // configuration, leaving the lifecycle at stopped when it refuses. The
    // resource phases are undone by rollback_start(); start_rollback runs that
    // undo on any exit between the first acquired resource and success.
    bool validate_start_config() noexcept;
    void warn_deadline_granularity() noexcept;
    void warn_fd_ceiling() noexcept;
    bool start_reactors(int n_reactors) noexcept;
    bool start_accept_and_handlers() noexcept;
    void rollback_start() noexcept;
    struct start_rollback {
        explicit start_rollback(multi_reactor_server* s) noexcept
            : server(s) {}
        start_rollback(const start_rollback&) = delete;
        start_rollback& operator=(const start_rollback&) = delete;
        start_rollback(start_rollback&&) = delete;
        start_rollback& operator=(start_rollback&&) = delete;
        ~start_rollback() { if (armed) server->rollback_start(); }
        void dismiss() noexcept { armed = false; }
        multi_reactor_server* server;
        bool armed = true;
    };

    bool        init_listen_socket() noexcept;
    // The one place the listening socket is closed, so that "no socket" and "no
    // port" cannot drift apart: a port() that outlives its socket reads like a
    // server that is still there.
    void        close_listen_socket() noexcept;
    void        accept_loop(std::promise<bool>& ready) noexcept;
    // accept_loop's parts. open/close own the four descriptors this thread
    // holds; accept_cleanup guarantees the close runs on every exit.
    bool        open_accept_descriptors() noexcept;
    void        close_accept_descriptors() noexcept;
    enum class accept_salvage { progress, backoff, stop };
    accept_salvage salvage_descriptor_exhaustion(int accept_errno) noexcept;
    void        admit_accept(int cfd, const struct sockaddr_storage& ss,
                             socklen_t slen) noexcept;
    struct accept_cleanup {
        multi_reactor_server* server;
        ~accept_cleanup() { server->close_accept_descriptors(); }
    };
    sub_reactor* pick_reactor() noexcept;
    // Called by a reactor whose loop is leaving through a failure. Marks the
    // reactor quarantined, counts it, and stops the server only when it was the
    // last live one. The accept thread is woken so it reaps the dead reactor
    // promptly. Exactly one call per failed reactor: the loop has one catch and
    // one non-EINTR epoll_wait exit, and both break out.
    void on_reactor_failed(sub_reactor* r) noexcept;
    // Accept-thread-only reaper: joins each dead reactor once, closing its
    // epoll fd and eventfd. reactors_ is stable while the accept thread runs,
    // and stop()/stop_gracefully() join that thread before touching the vector,
    // so the join below never races another join.
    void reap_dead_reactors() noexcept;
    // Per-reactor slots: allocated once at the maximum reactor count so a
    // changed resolved count between restarts can never index past them.
    bool ensure_reactor_slots(size_t n) noexcept;
    void check_reactor_stalls() noexcept;
    // How long the accept thread may park in epoll_wait. With the watchdog
    // enabled it must sample faster than the timeout, or a stall that ends
    // between two samples is never observed; 500 ms is the ceiling because
    // this thread also reaps dead reactors and drains admissions.
    int  accept_wait_ms() const noexcept;
    std::unique_ptr<reactor_slot[]> reactor_slots_;
    // Slots [0, reactor_slot_count_) are exposed. It is the high-water mark
    // of every resolved reactor count, never shrunk, so cumulative traffic in
    // slots retired by a smaller run stays visible and reconciles with
    // server_stats().
    std::atomic<size_t>             reactor_slot_count_{0};
    // Slots [0, reactor_run_count_) belong to the current (or most recent
    // successful) run; slots above it are retired. Zero after a failed start.
    std::atomic<size_t>             reactor_run_count_{0};
    std::atomic<reactor_slot*>      published_slots_{nullptr};

    config cfg_;
    int listen_fd_ = -1;
    int epoll_fd_  = -1;
    int wake_fd_   = -1;
    // One descriptor held in reserve so that EMFILE can be answered instead of
    // waited out (see salvage_descriptor_exhaustion). Created by
    // open_accept_descriptors() under accept_fd_mutex_, accept-thread-owned
    // after that, and closed with the other two on the way out.
    int spare_fd_  = -1;
    // Rate limit for the out-of-descriptors warning: a storm would otherwise
    // print once per failed accept, which is hundreds of lines a second.
    int64_t last_fd_log_ms_ = 0;
    // Resolved from the bound socket, never from cfg_. The obvious alternative
    // -- writing it back into cfg_.port after bind() -- makes the next start()
    // after a stop() bind last run's throwaway number instead of asking for a
    // fresh one, which is a worse surprise than the one being fixed.
    std::atomic<uint16_t> bound_port_{0};

    std::atomic<life>     life_{life::stopped};
    std::atomic<bool>     failed_{false};
    // True while the accept thread may hand new connections to reactors.
    // Cleared at the start of a graceful drain, so no new connection is
    // admitted while it runs; stop() clears it as well.
    std::atomic<bool>     accepting_{false};
    std::atomic<uint64_t> next_conn_id_{1};
    std::atomic<size_t>   next_reactor_{0};
    // How many reactors are still alive. start_reactors() sets it to the full
    // set before any reactor thread exists; on_reactor_failed() decrements it,
    // and the decrement that takes it to 0 is what fails the server. This
    // single atomic is the last-live decision: scanning per-reactor flags after
    // a store would be a Dekker race that weak memory models are free to lose.
    std::atomic<size_t>   live_reactors_{0};
    // Serializes start() and stop(); stop_gracefully() only takes it through
    // its final stop(), so it assumes the same controlling thread as those two
    // and must not run concurrently with them. Submissions hold their barrier
    // only until queued. stop releases it before joining so worker callbacks
    // can finish.
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
    // Admission hardening, resolved by start() from the configuration.
    size_t                       per_conn_buffer_bytes_ = 0;
    size_t                       max_buffer_bytes_ = 0;
    std::shared_ptr<ip_admission> ip_table_;

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
    if (server_->fail_reactor_start_after_for_test_.load(
            std::memory_order_relaxed) >= 0) {
        if (server_->fail_reactor_start_after_for_test_.fetch_sub(
                1, std::memory_order_relaxed) == 0) {
            server_->fail_reactor_start_after_for_test_.store(
                -1, std::memory_order_relaxed);
            return false;
        }
    }
    if (running_.exchange(true)) return true;

    reactor_slot* slots =
        server_->published_slots_.load(std::memory_order_acquire);
    slot_ = slots ? &slots[idx_] : nullptr;

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
    ev.events = EPOLLIN; ev.data.u64 = REACTOR_WAKE_TAG;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
        ::close(wake_fd_); ::close(epoll_fd_);
        wake_fd_ = epoll_fd_ = -1; running_.store(false); return false;
    }

    // Computed here rather than in the constructor: the reactor count and the
    // connection ceiling are both decisions the server makes later, and this is
    // the first point at which both are final.
    const size_t share =
        static_cast<size_t>(server_->cfg_.max_connections) /
        static_cast<size_t>(server_->cfg_.resolved_reactor_count());
    pending_task_cap_ = std::max(k_min_pending_tasks, share * 4);
    data_task_cap_ = pending_task_cap_ - k_control_task_reserve;
    // Both batch vectors reserve the cap up front, so the swap in
    // process_pending() never allocates.
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

inline void sub_reactor::cleanup(const char* reason) noexcept {
    multi_reactor_server::callback_scope callbacks(server_);
    // Discard waiting tasks. A new_conn_task's fd_guard closes an unclaimed
    // socket; a send_task's frame vector is freed with the task.
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        server_->stats_ref().pending_tasks_depth.fetch_sub(
            pending_tasks_.size(), std::memory_order_relaxed);
        // The send_tasks dropped here already counted their frames into
        // pending_bytes when they were submitted, and that reservation is not
        // given back. It is not a leak: pending_bytes is per connection and
        // every connection is torn down below, so the count dies with its
        // owner. It does mean bytes_dropped and queue_drops do not see these
        // frames -- at shutdown there is no connection left for them to be
        // about, which is the same reason discard_pending_output() owns only
        // the wbuf half.
        pending_tasks_.clear();
    }

    while (!conns_.empty()) {
        // Shutdown cannot wait out anyone's half-close -- the reactor is
        // leaving and there is no thread left to deliver a parked connection's
        // last bytes. Abort explicitly, and account the undelivered bytes as
        // drops rather than pretending they went out.
        std::shared_ptr<connection> c = conns_.begin()->second;
        close_conn(c.get(), reason, false);
    }
    flush_close_callbacks();
    active_tasks_.clear();
    conns_.clear();
    // The slot is read by operators after a stop/quarantine; leaving the last
    // published count behind reports connections that no longer exist.
    if (slot_) slot_->connections.store(0, std::memory_order_relaxed);
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
        // The tasks left after a break are dropped with their send_task
        // frames; their pending_bytes reservations are not returned (cleanup()
        // explains why that cannot leak and is not counted).
        if (!running_.load() || !server_->is_running()) break;
        // One indirect call through the table, indexed by the alternative.
        k_task_dispatch[t.index()](this, t);
        flush_close_callbacks();
    }
    active_tasks_.clear();
    flush_close_callbacks();
}

inline bool sub_reactor::enqueue(queue_task task) noexcept {
    // A reply flood stops at data_task_cap_; close/drain/park and
    // new-connection tasks may use the reserved slots up to the full cap.
    // Same deque, so submission order (answer, then close) is unchanged.
    // Both caps are written before this reactor's thread starts and never
    // change while it runs, so reading them here, outside the mutex, is safe.
    const size_t cap = std::holds_alternative<send_task>(task)
                           ? data_task_cap_ : pending_task_cap_;
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        if (!running_.load() || !server_->is_running() ||
            pending_tasks_.size() >= cap) {
            server_->stats_ref().queue_drops.fetch_add(
                1, std::memory_order_relaxed);
            return false;
        }
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
        c->sm.attach(&c->wbuf, &c->rbuf, &c->pending_bytes);

        struct epoll_event ev{};
        ev.events = EPOLLIN; ev.data.u64 = t.id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, t.fd.fd, &ev) < 0) {
            server_->stats_ref().epoll_ctl_errors.fetch_add(
                1, std::memory_order_relaxed);
            server_->stats_ref().rejected.fetch_add(
                1, std::memory_order_relaxed);
            return;  // The task's fd_guard closes the descriptor.
        }
        c->fd = t.fd.release();
        if (server_->cfg_.socket_send_buffer_bytes > 0) {
            const size_t asked = server_->cfg_.socket_send_buffer_bytes;
            int sndbuf = asked > static_cast<size_t>(INT_MAX)
                             ? INT_MAX
                             : static_cast<int>(asked);
            // Best-effort: the kernel clamps to its own minimum, and a
            // failure here only means the OS default is kept.
            setsockopt(c->fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        }

        try {
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
    // A parked close still accepts the reply queued before it: the close task
    // only marks the connection when it runs, and it runs after this one in the
    // same batch. Anything arriving after that point is refused here
    // -- the FIN is already reserved for what we owe the peer.
    if (c->sm.refuses_sends()) {
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
        maybe_resume_read(c);
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
        maybe_resume_read(c);
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
    if (c->sm.refuses_sends()) {
        *out = send_result::closed;
        return true;
    }
    // A close request is already ahead of this send: take the queued path so
    // the frame is ordered after it and dropped, exactly as before.
    if (c->sm.close_request_pending())
        return false;

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
    if (server_->fail_close_enqueue_for_test_.load(std::memory_order_relaxed)) {
        server_->fail_close_enqueue_for_test_.store(
            false, std::memory_order_relaxed);
        return false;
    }
    return enqueue(queue_task{close_task{id}});
}

inline void sub_reactor::handle_task(close_task& task) noexcept {
    const uint64_t id = task.id;
    auto it = conns_.find(id);
    if (it != conns_.end()) {
        // Hold the map's reference across the whole call. teardown_closed()
        // hands its own reference to the deferred-close entry, so if queueing
        // that entry throws, the entry's destructor would drop the last
        // reference -- while drain_input_then_close() is still reading the
        // connection. Holding it here costs nothing: the callbacks only run in
        // flush_close_callbacks() below, after this scope is gone.
        std::shared_ptr<connection> c = it->second;
        // Default (graceful): this is an application-level "we are done", so
        // the reply queued just before it must still reach the peer -- and so
        // must answers to requests the peer already sent but this reactor has
        // not read yet.
        drain_input_then_close(c.get());
    }
    flush_close_callbacks();
}

inline void sub_reactor::handle_task(finish_peer_closed_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    // Same hold as close_task: finish_peer_closed() can reach
    // teardown_closed(), whose notification allocation may fail and drop the
    // last reference. Its teardown calls are tail actions today -- nothing
    // reads the connection after them -- so this is not a live bug, but the
    // hold keeps that from being an invariant the next edit has to preserve
    // (R9-1).
    std::shared_ptr<connection> keep = it->second;
    finish_peer_closed(keep.get());
}

inline void sub_reactor::handle_task(drain_input_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    // Same reason as close_task above: the drain can reach teardown_closed(),
    // whose notification allocation may fail, and this frame must still own
    // the connection while drain_input_then_close() reads it (R9-1).
    std::shared_ptr<connection> keep = it->second;
    drain_input_then_close(keep.get());
}

inline void sub_reactor::handle_task(park_close_task& task) noexcept {
    auto it = conns_.find(task.id);
    if (it == conns_.end()) return;
    // Same hold as close_task: park_close() can reach teardown_closed(), whose
    // notification allocation may fail and drop the last reference. Its
    // teardown calls are tail actions today, so this is not a live bug, but the
    // hold keeps that from being an invariant the next edit has to preserve
    // (R9-1).
    std::shared_ptr<connection> keep = it->second;
    park_close(keep.get(), "requested");
}

// The one place that turns the connection's interest state into an epoll
// registration. Every writer goes through it, or a pause can be silently
// undone: arm_write used to set EPOLLIN unconditionally, and resume_read on a
// peer-FIN'd parked socket would restore the busy-poll the park is there to
// avoid. fail_reason names the caller for the epoll_ctl_errors close.
inline void sub_reactor::apply_interest(connection* c,
                                        const char* fail_reason) noexcept {
    if (c->fd < 0) return;
    struct epoll_event ev{};
    ev.data.u64 = c->id;
    ev.events = c->sm.interest();
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, c->fd, &ev) == 0) return;
    if (errno == ENOENT) {
        // Registered set out of sync (fd was re-added elsewhere): restore it
        // with ADD.
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, c->fd, &ev) == 0) return;
    }
    server_->stats_ref().epoll_ctl_errors.fetch_add(
        1, std::memory_order_relaxed);
    close_conn(c, fail_reason, false);
}

inline void sub_reactor::arm_write(connection* c) noexcept {
    // Nothing asked for can ever leave a dead write side, and asking for it
    // each time bytes arrive would spin this reactor on level-triggered
    // EPOLLOUT for the rest of the connection's life.
    if (c->fd < 0 || !c->sm.on_set_write(true)) return;
    apply_interest(c, "epoll write registration failed");
}

inline void sub_reactor::disarm_write(connection* c) noexcept {
    // Target state, not a toggle: the machine's write flag says what the
    // EPOLLOUT bit should be, so the transition is skipped when the bit is
    // already right and issued when it is not. The two are not the same
    // question -- a park_close that drains completely sends its FIN without
    // having disarmed, so the flag can still be true past our shutdown, and
    // skipping the MOD on the stale EPOLLOUT that follows would leave a
    // half-closed connection spinning on a level-triggered event it can never
    // consume.
    if (c->fd < 0) return;
    // No "still has bytes to flush" guard belongs here: dropping the interest
    // while wbuf holds bytes is exactly what strands them, and both callers
    // (handle_write, after the drain emptied wbuf, and the fin_sent path) have
    // already established there is nothing left to write. Leaving it to the
    // caller is what keeps this function a plain state transition.
    if (!c->sm.on_set_write(false)) return;
    apply_interest(c, "epoll read registration failed");
}

// Stop reading from the kernel: the peer has more requests than we are willing
// to have in flight. Dropping EPOLLIN is what applies TCP backpressure; the
// read side resumes from the drain or from the tick fallback.
inline void sub_reactor::pause_read(connection* c) noexcept {
    // The machine refuses when a pause may not happen: already paused, peer
    // FIN seen, parked or closed, or inside a close's serving round.
    if (c->fd < 0 || !c->sm.on_pause(now_ms())) return;
    // A continuation queued before the pause would keep the loop on a zero
    // epoll timeout; take it off the list now.
    unschedule_read(c);
    server_->stats_ref().read_pauses.fetch_add(1, std::memory_order_relaxed);
    apply_interest(c, "epoll pause registration failed");
}

inline void sub_reactor::resume_read(connection* c) noexcept {
    apply_resume(c, c->sm.on_resume(now_ms()));
}

// The reactor's half of a resume, for both the explicit one above and the one a
// close implies (the machine resumes a paused connection as it enters closing
// or draining, because the combination is not a state).
inline void sub_reactor::apply_resume(connection* c,
                                      const read_resume& r) noexcept {
    if (!r.resumed) return;
    // The assembly deadline measures peer dribbling, so the paused interval is
    // not charged to the peer -- but it is added to the deadline, not turned
    // into a fresh one. Resetting the stamp would let a peer that alternates
    // between reading a little and stalling discard the time it already had.
    if (c->frame_started_at_ms != 0) c->frame_started_at_ms += r.paused_for_ms;
    apply_interest(c, "epoll resume registration failed");
    if (c->sm.closed() || c->sm.peer_closed()) return;
    // Frames already in rbuf are not going to produce another EPOLLIN: the
    // kernel had its say when they arrived. Queue a continuation so they are
    // served now, not at the next unrelated event.
    if (c->rbuf.readable() > 0) schedule_read(c);
}

inline void sub_reactor::maybe_resume_read(connection* c) noexcept {
    if (!c->sm.read_paused()) return;
    if (c->pending_bytes.load(std::memory_order_relaxed) <=
        server_->cfg_.resolved_read_low_watermark_bytes())
        resume_read(c);
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
        c->last_drain_progress_ms = now_ms();
        // The peer is keeping up: reopen the read side once the backlog has
        // fallen to the low mark.
        maybe_resume_read(c);
    }
    return drain_outcome::drained;
}

// Reads and parsing interleave under a per-call budget: no single client can
// own this reactor, and a full rbuf that still holds whole frames is never
// mistaken for a peer that has gone quiet.
inline int sub_reactor::handle_read(connection* c) noexcept {
    unschedule_read(c);
    // A paused connection may still be invoked: an event already in the batch,
    // or a continuation queued before the pause was applied. Do not read, and
    // do not reschedule (schedule_read refuses while paused too).
    if (c->sm.read_paused()) return 0;
    constexpr int k_read_budget  = 64 * 1024;
    constexpr int k_frame_budget = 256;
    int bytes_left = k_read_budget, frames_left = k_frame_budget;
    // Frames this call parsed and served. Every exit reports it, because
    // "the peer asked nothing more" and "the peer asked, and answers for it are
    // now queued behind whichever task called us" are not distinguishable from
    // outside -- and only the second one has something to wait for.
    auto dispatched = [&frames_left] { return k_frame_budget - frames_left; };

    for (;;) {
        if (!server_->is_running()) return dispatched();
        // Parse whatever is already a complete frame
        parse_frames(c, frames_left);
        if (c->sm.closed())
            return dispatched();
        // parse_frames may have paused us at a frame boundary (or the PING
        // reservation did). Either way, do not go on to recv().
        if (c->sm.read_paused()) return dispatched();
        // The peer's FIN has already been read and finish_peer_closed() owns
        // what happens next. recv() here can only report the same EOF again,
        // and that second observation is not harmless: if it lands while the
        // peer-owned round is still queued (behind the replies it
        // exists to wait for), on_peer_fin() reads the round as spent and
        // tears down over them. A close's drain round is exactly such a
        // caller, so stop after parsing.
        if (c->sm.peer_closed()) return dispatched();
        // No frame was buffered, but output is already over the high mark:
        // stop before the syscall, which is what lets TCP push back. The
        // close's own drain round is exempt for the same reason as
        // parse_frames: the close budget bounds it (see conn_state::in_round).
        if (c->sm.backpressure_applies() &&
            c->pending_bytes.load(std::memory_order_relaxed) >=
                server_->cfg_.resolved_read_high_watermark_bytes()) {
            pause_read(c);
            return dispatched();
        }
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
            c->sm.on_eof();
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
    if (c->sm.fin_sent()) {
        disarm_write(c); return;
    }
    const drain_outcome r = drain_wbuf_to_socket(c);
    const close_action a = conn_sm::on_drain_result(r);
    if (a == close_action::close_abort) {
        // Nothing more will ever leave this socket, so there is nothing for the
        // rest of this function to wait for: no FIN can be negotiated (it
        // requires an empty wbuf first) and no later EPOLLOUT can deliver what
        // is queued. Ending it here rather than waiting is also what keeps this
        // from becoming the write-side echo of the busy-poll the read side was
        // cured of -- a socket with send-buffer space and a broken peer reports
        // EPOLLOUT on every single epoll_wait, and leaving the interest armed
        // would have this reactor re-run a failing sendmsg until some outside
        // deadline noticed.
        const char* reason =
            r == drain_outcome::dead ? "write side dead" : "send error";
        close_conn(c, reason, false);
        return;
    }
    // fair-share budget or EAGAIN: EPOLLOUT refires
    if (a == close_action::none) return;

    // A parked peer-close finishes here, deterministically, instead of waiting
    // for the next spurious EPOLLIN to notice the drain completed.
    if (c->sm.peer_closed()) {
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
    if (c->sm.closing() &&
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
    if (c->sm.fin_sent() || c->fd < 0) return c->sm.fin_sent();
    // Never FIN over bytes still in wbuf: they would be lost, and the peer
    // would see a clean end to a truncated stream.
    if (!c->sm.wbuf_empty()) return false;
    if (::shutdown(c->fd, SHUT_WR) < 0) return false;
    c->sm.on_fin_sent();
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
//      batch. Defer exactly one queue round (close_round::peer_closed) so
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
    if (c->sm.closed()) return;
    if (!c->sm.wbuf_empty()) {
        // Parked with the write side still productive. Read interest is dropped
        // for the rest of the drain (see the header comment): the only thing a
        // read could deliver is the FIN we have already seen. apply_interest
        // derives that from the machine's read mode (eof), and still
        // recovers an ENOENT
        // registration with ADD -- without that recovery the socket goes
        // silent and the drain never resumes (measured: 3MB of 16MB, then
        // nothing). A registration the kernel will not accept ends the
        // connection: EPOLLIN left armed on a FIN'd socket is a read event on
        // every epoll_wait, each one re-running recv() -> 0 -> this same
        // failing call (measured: ~170k/s, one core, until the linger abort).
        if (c->fd >= 0) {
            c->sm.on_set_write(true);
            apply_interest(c, "epoll park registration failed");
            return;                    // parked: handle_write resumes it
        }
        // fd < 0 means the descriptor is already detached, and the only place
        // that detaches it is teardown_closed() -- which enters phase closed
        // first,
        // so the guard at the top of this function has already returned.
        // Reaching here would mean a second detach path exists; the guard above
        // is kept so that adding one cannot turn into a blind epoll_ctl on fd
        // -1.
        return;                    // parked close finishes in handle_write
    }
    switch (c->sm.on_peer_fin()) {
      case close_action::defer_peer_closed: {
        const uint64_t id = c->id;
        if (enqueue(queue_task{finish_peer_closed_task{id}})) return;
        // The hand-off failed (queue full, or the reactor is going down): a
        // late FIN beats an answer nobody can wait for any longer.
        break;
      }
      case close_action::send_fin:
        // Still closing on our side and the peer declared itself done too: the
        // only thing left is the FIN. Anything a handler queued was drained
        // above or is still passing through the socket buffer, ahead of it.
        if (!try_begin_write_shutdown(c)) {
            teardown_closed(c, "peer closed", false); return;
        }
        break;
      case close_action::close_graceful:
        break;
      default:
        return;   // none: closed was already checked at the top
    }
    teardown_closed(c, "peer closed", true);
}

inline void sub_reactor::parse_frames(connection* c,
                                      int& frames_left) noexcept {
    uint8_t hdr[FRAME_HEADER_SIZE];
    // Closed is checked in the loop, not only by the callers. A message
    // handler runs application code between frames, and that code may call
    // send_to(); if its arm_write() fails, the connection is torn down before
    // this loop gets back to the header. Continuing would keep parsing and
    // dispatching frames on a connection the reactor has already retired.
    while (server_->is_running() && frames_left > 0 &&
           !c->sm.closed() &&
           c->rbuf.readable() >= FRAME_HEADER_SIZE) {
        // Read backpressure stops here, at a frame boundary with the header
        // still untouched, so the frame can be retried intact after resume. A
        // graceful close overrides it: close_input_bytes and close_linger_ms
        // are what bound input during a close. The machine's in_round flag
        // marks the
        // narrow window where the close is serving that backlog; without it
        // the pause reapplies before the first frame and the drain parks over
        // exactly what it was asked to answer.
        if (c->sm.backpressure_applies() &&
            c->pending_bytes.load(std::memory_order_relaxed) >=
                server_->cfg_.resolved_read_high_watermark_bytes()) {
            pause_read(c);
            return;
        }
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
        // The type is in the header, so an unknown one is settled here rather
        // than after the payload arrives. It is checked before the length: an
        // unknown type means the length field is not ours to interpret, so a
        // foreign header with an arbitrary length is a protocol mismatch, not
        // a framing error. Waiting would also let such a peer hold the
        // connection -- and up to a full frame of rbuf -- until the assembly
        // deadline. A connection paused by read backpressure is the exception:
        // no frame, this header included, is looked at until it resumes.
        if (type != frame_type::DATA && type != frame_type::PING &&
            type != frame_type::PONG &&
            !server_->cfg_.allow_unknown_frame_types) {
            server_->stats_ref().unknown_frame_types.fetch_add(
                1, std::memory_order_relaxed);
            close_conn(c, "unknown frame type", false);
            return;
        }
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

        // A PING has to reserve its PONG before the frame is committed. A
        // failed reservation then leaves the frame in rbuf to retry after
        // resume -- and a retried frame must not have been counted as
        // received. A closing connection reserves nothing: its PING is dropped
        // below, and reserving just to subtract again would be a window in
        // which a PONG could stop a FIN.
        bool pong_reserved = false;
        if (type == frame_type::PING && !c->sm.closing()) {
            const auto& pong = pong_frame();
            size_t pending = c->pending_bytes.load(std::memory_order_relaxed);
            while (pending <= server_->cfg_.max_pending_bytes &&
                   pong.size() <= server_->cfg_.max_pending_bytes - pending) {
                if (c->pending_bytes.compare_exchange_weak(
                        pending, pending + pong.size(),
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    pong_reserved = true;
                    break;
                }
            }
            // The loop-top high-mark check should have made this unreachable;
            // an explicit high mark equal to max_pending_bytes can leave a
            // window. Pause and retry the same frame instead of resetting a
            // peer whose only fault is reading slowly.
            //
            // A close's drain round is the exception. `closing` is still false
            // while drain_input_then_close() serves the backlog -- park_close()
            // sets it one step later -- so this branch is reachable inside that
            // window, and pausing there would strand the very round that exists
            // to answer the input. Fall through to the drop path below instead:
            // the close owns this frame, and a PING whose PONG cannot be
            // admitted under its own limit is owed nothing.
            if (!pong_reserved) {
                if (c->sm.backpressure_applies()) {
                    pause_read(c);
                    return;
                }
            } else if (c->wbuf.writable() < pong.size()) {
                // Unreachable by the pending<=max_pending<=wbuf_capacity
                // invariant; kept because the alternative to this branch is a
                // partial frame in the stream.
                c->pending_bytes.fetch_sub(
                    pong.size(), std::memory_order_relaxed);
                close_conn(c, "wbuf full on pong", false); return;
            }
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
            if (!pong_reserved) {
                // Mid-close: the FIN is already (or about to be) behind
                // everything we owe the peer. Queueing a PONG after it would
                // either be sent past the FIN -- which the kernel refuses --
                // or silently lost. The peer is entitled to nothing more than
                // the reply it asked for.
                server_->stats_ref().frames_dropped_on_close.fetch_add(
                    1, std::memory_order_relaxed);
                c->rbuf.consume(len);
                continue;
            }
            const auto& pong = pong_frame();
            c->wbuf.write(pong.data(), pong.size());
            // frames_sent is likewise counted on leaving wbuf (see
            // submit_send). Idempotent move toward a target state; no
            // "was_empty" test either.
            arm_write(c);
            // close_conn only marks and unlists now and runs no callback on
            // this stack -- though a failed arm just above may already have
            // closed it.
            if (c->sm.closed()) return;
            c->rbuf.consume(len);
        } else if (type == frame_type::PONG) {
            c->rbuf.consume(len);
        } else {
            // DATA, or an unknown type the application opted into delivering
            // as DATA. The header check above rejected every other one, so no
            // further type test is needed here.
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
            if (mh) {
                try {
                    mh(c->id, frame_scratch_.data(), len);
                } catch (...) {
                    // The same contract violation the open state handler
                    // absorbs (R8-2): the frame has been consumed, the
                    // connection is healthy, and there is nothing to undo or
                    // to count. Letting it cross parse_frames() -- noexcept --
                    // would call std::terminate() and take every connection
                    // with it, not just the one whose message it was.
                }
            }
        }
    }

    // A header that has not arrived in full is also a frame in progress.
    // Nothing is stamped once the connection is closed: there is no assembly
    // deadline left to run on it.
    if (c->sm.closed()) return;
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
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, c->fd, nullptr);
    ::close(c->fd);
    c->fd = -1;
}

// A connection that was accepted and then refused by its own registration --
// the map insert threw, or on_connection_opened said no. unregister_conn()
// already took the descriptor and the epoll registration, and used to be the
// whole of the rollback: the conns_ entry survived it as a
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
    c->sm.on_rejected();
    // The task holds the last reference and would release the ticket on return
    // either way; releasing it here instead frees the admission slot now rather
    // than whenever the ghost was going to be reaped.
    c->admission.reset();
}

// pending_bytes is reserved when a frame is admitted and released wherever that
// frame stops being owed: written to the kernel (drain_wbuf_to_socket), dropped
// whole (capacity reject, closing connection, refused PONG), or rolled back
// because the submission failed. The bytes still sitting in wbuf are the case
// handled here: the connection is going away, so they will never be sent, and
// the reservation must be subtracted or it leaks for the lifetime of the
// connection -- and any watermark built on it stays permanently inflated.
//
// Bytes still reserved by queued-but-unwritten send tasks are NOT handled here:
// the send task itself releases them when it runs against a closed connection,
// so this function must never try to guess at them (it would double-count).
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
    if (graceful && !c->sm.fin_sent()) {
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
    const close_step step = c->sm.on_graceful(now_ms());
    if (step.action == close_action::none) return;
    // A close overrides read backpressure. The FIN exchange needs read
    // interest to see the peer's FIN even when this connection was paused;
    // close_linger_ms bounds how long that wait may last. The machine has
    // already left the paused state (closing and paused do not coexist); this
    // is the reactor applying it.
    apply_resume(c, step.resume);
    // The resume can end the connection (an epoll_ctl failure aborts it).
    if (c->sm.closed()) return;
    // Hand over what is already queued; whatever does not fit in the socket
    // buffer stays in wbuf and is finished by handle_write (EPOLLOUT armed).
    const drain_outcome r = drain_wbuf_to_socket(c);
    switch (conn_sm::on_drain_result(r)) {
      case close_action::close_abort:
        // Once the socket refuses writes for good there is no graceful close
        // left to honour -- nothing owed can leave, so no FIN can honestly
        // follow it. Waiting would change nothing except which deadline does
        // this same abort later.
        teardown_closed(c, reason, false); return;
      case close_action::none:
        arm_write(c); return;   // stalled: wbuf non-empty, EPOLLOUT resumes
      case close_action::drain_complete:
        break;   // wbuf empty, FIN goes out below
      default:
        break;   // unreachable: on_drain_result returns only the three above
    }
    // wbuf empty: either every byte is in the socket buffer, or sendmsg stopped
    // exactly on the budget boundary. In both cases nothing more can be queued
    // (`closing` refuses submissions), so this is where the FIN goes out. If
    // shutdown() fails there is no FIN to wait for, and do_tick's close_linger
    // deadline becomes the only thing left to end the connection.
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
//
// WHAT THE BUDGET DOES NOT COVER. Requests still in the kernel receive buffer
// when the close was requested are not counted -- close_input_bytes is rbuf
// occupancy, and at that moment the kernel's copy is invisible. The drain does
// resume reads, and `draining_input` exempts this one round from the output
// watermark, so it pulls up to one read budget (64 KiB / 256 frames) of them
// into service while `closing` is still false. The connection's ordinary read
// continuation may serve more before the park lands, but that path is not
// exempt: it stops at the high watermark. Neither is a promise. Measured
// (verify/evidence/probe_close_while_paused.cpp): a
// read-paused connection with 4000 requests sitting in the kernel answered 256
// of them -- one round -- and then FIN. All 4000 were parsed and handed to the
// handler; the replies past the prefix were refused once `closing` was set, and
// the peer received an orderly FIN over them. frames_recv said 4000, the wire
// said 256, and only the wire is what the peer can rely on.
//
// Widest gap: a connection that was read-paused. The pause exists precisely to
// leave bytes in the kernel, so at close time most of the backlog is the part
// that is not counted -- and the close lifts the pause only to spend its own
// round on it.
//
// That is the deliberate bound. Counting the kernel buffer would mean
// FIONREAD on every close; draining it would make the close wait on a pipe the
// peer controls, with close_linger_ms as the only deadline left. Either turns a
// local decision ("we are done with this peer") into a remote one, which is the
// behaviour the budget exists to prevent.
inline void sub_reactor::drain_input_then_close(connection* c) noexcept {
    const close_step step = c->sm.on_request(now_ms());
    if (step.action == close_action::none) return;
    // A close overrides read backpressure: the budget below (measured from
    // what was already buffered) bounds how much this may serve, and the peer
    // is owed answers to the requests it sent before the close. The machine
    // has already left the paused state; this is the reactor applying it.
    apply_resume(c, step.resume);
    // The resume can end the connection (an epoll_ctl failure aborts it), and
    // serving a backlog on a dead connection would hand its frames to the
    // application for nothing.
    if (c->sm.closed()) return;

    c->drain_read_bytes = 0;
    const size_t queued_before = c->rbuf.readable();
    // The close's own budget bounds this round, not the output high mark: a
    // connection that was paused before the close must still serve what it
    // already buffered, or parking would FIN over those requests. Handlers
    // run with `closing` still false, so their replies are accepted (or
    // refused as overloaded by send_to, which is a counted answer rather
    // than a teardown).
    c->sm.on_round_begin();
    const int served = handle_read(c);
    c->sm.on_round_end();
    // handle_read has its own exits: the peer's FIN may have arrived in the
    // same read, or the socket may have errored. Either way the connection is
    // already accounted for and there is nothing left for this request to do.
    if (c->sm.closed() ||
        c->sm.fin_sent())
        return;

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
    int64_t dealt = 0;
    if (served > 0) {
        dealt = static_cast<int64_t>(queued_before)
              + static_cast<int64_t>(c->drain_read_bytes)
              - static_cast<int64_t>(c->rbuf.readable());
        c->drain_read_bytes = 0;
    }

    // The decisions below live in conn_sm::on_input_drained. The
    // defer_drain_input and defer_park cases are the two "answers are queued
    // behind this batch" branches; none is the peer_closed round already
    // being owned. Progress is required (served > 0) so a peer that stops
    // mid-frame cannot hold the close open on a half a frame. Case F's "11 of
    // 22 bytes" is the defer_park case: parking early would set `closing`,
    // submit_send refuses the reply, and the peer gets an orderly FIN over a
    // request we took in, handled, and answered into nothing.
    close_action a = c->sm.on_input_drained(served, dealt);
    if (a == close_action::defer_drain_input) {
        const uint64_t id = c->id;
        if (enqueue(queue_task{drain_input_task{id}})) return;
        // The hand-off failed: park below rather than keeping it pending.
        a = c->sm.on_input_defer_failed();
    }
    if (a == close_action::defer_park) {
        const uint64_t id = c->id;
        if (enqueue(queue_task{park_close_task{id}})) return;
        // The hand-off failed: park here rather than keeping it forever.
    } else if (a != close_action::drain_wbuf) {
        return;   // none: peer_closed round owns it, or already done
    }
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
    if (!c->sm.on_closed()) return;
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
    // EPOLL_CTL_DEL + close + fd = -1
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
        // Each callback is contained separately: a throw from the state
        // handler must not suppress the close notification, and either one is
        // an application bug rather than a reason to terminate a server that
        // has already torn the connection down.
        if (sh) {
            try {
                sh(e.id, e.peer, false);
            } catch (...) {
                server_->stats_ref().callback_exceptions.fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
        if (server_->close_handler_) {
            try {
                server_->close_handler_(e.id, e.peer, e.reason.c_str());
            } catch (...) {
                server_->stats_ref().callback_exceptions.fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
    }
}

inline void sub_reactor::schedule_read(connection* c) noexcept {
    // A paused connection must not enter the continuation list: the loop runs
    // continuations with a zero epoll timeout, so a paused entry would turn
    // every iteration into a spin that handle_read immediately declines.
    if (c->sm.read_paused() || c->read_pending) return;
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
    std::vector<connection_ptr> wanted;    // queue refused; sweep owes a close
    std::vector<connection_ptr> resumable;  // paused, backlog below low
    std::vector<connection_ptr> idle;      // graceful: may still owe bytes
    std::vector<connection_ptr> expired;   // past its deadline: forced
    std::vector<connection_ptr> stalled;   // frame never completed
    for (auto& kv : conns_) {
        connection* c = kv.second.get();
        // close_connection() could not queue its task. The request still
        // stands, so perform the same drain-then-close here. Checked before
        // the deadline sweeps: a requested close must not be reclassified as
        // an idle timeout, and the connection may already be closing (then the
        // flag is just a stale retry and the deadline logic below owns it).
        if (c->sm.take_close_wanted() && !c->sm.closing()) {
            wanted.push_back(kv.second);
            continue;
        }
        if (c->sm.closing()) {
            // A closing connection has a deadline regardless of which stage it
            // reached, and it is the same value for both: how long the close
            // may take. last_recv_ms is useless here (the peer has nothing left
            // to send), so measure from the close itself; park_close stamps
            // close_started_at_ms before any of the stages that can stall.
            if (linger > 0 && c->fd >= 0 && c->sm.close_started_at_ms() != 0 &&
                now - c->sm.close_started_at_ms() >= linger)
                expired.push_back(kv.second);
            continue;
        }
        // A paused connection resumes here when the backlog has fallen to the
        // low mark. The drain path resumes immediately; this covers releases
        // that happen on a caller thread (send_to's rollback), which cannot
        // touch this reactor's epoll registration.
        if (c->sm.read_paused()) {
            if (c->pending_bytes.load(std::memory_order_relaxed) <=
                server_->cfg_.resolved_read_low_watermark_bytes()) {
                resumable.push_back(kv.second);
                continue;
            }
            // Still backpressured. The idle check below still applies, but it
            // must count drain progress as activity: while paused, no inbound
            // bytes are stamped, and a peer that is reading steadily is not
            // idle just because it is slow.
        }
        const int64_t last_activity =
            std::max(c->last_recv_ms, c->last_drain_progress_ms);
        if (last_activity < cutoff) {
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
        // so the slot is released now. A paused connection is exempt: it is
        // not reading, so a frame left incomplete is our backpressure, not the
        // peer dribbling. resume_read() restarts the deadline.
        if (!c->sm.read_paused() && c->frame_started_at_ms != 0 &&
            frame_timeout > 0 &&
            now - c->frame_started_at_ms >= frame_timeout) {
            server_->stats_ref().frame_assembly_timeouts.fetch_add(
                1, std::memory_order_relaxed);
            stalled.push_back(kv.second);
        }
    }
    for (auto& c : wanted)    drain_input_then_close(c.get());
    for (auto& c : resumable) resume_read(c.get());
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
        if (slot_)
            slot_->frames_recv.fetch_add(
                frames_recv_local_, std::memory_order_relaxed);
        frames_recv_local_ = 0;
    }
    if (frames_sent_local_ != 0) {
        stats.frames_sent.fetch_add(
            frames_sent_local_, std::memory_order_relaxed);
        if (slot_)
            slot_->frames_sent.fetch_add(
                frames_sent_local_, std::memory_order_relaxed);
        frames_sent_local_ = 0;
    }
    if (bytes_recv_local_ != 0) {
        stats.bytes_recv.fetch_add(
            bytes_recv_local_, std::memory_order_relaxed);
        if (slot_)
            slot_->bytes_recv.fetch_add(
                bytes_recv_local_, std::memory_order_relaxed);
        bytes_recv_local_ = 0;
    }
    if (bytes_sent_local_ != 0) {
        stats.bytes_sent.fetch_add(
            bytes_sent_local_, std::memory_order_relaxed);
        if (slot_)
            slot_->bytes_sent.fetch_add(
                bytes_sent_local_, std::memory_order_relaxed);
        bytes_sent_local_ = 0;
    }
    if (slot_) slot_->connections.store(
        conns_.size(), std::memory_order_relaxed);
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
            // Progress is a completed loop iteration. Stamped at the top so
            // every path through the body, including an EINTR continue, counts;
            // a handler that blocks stops the stamp advancing.
            if (slot_)
                slot_->last_progress_ms.store(
                    now_ms(), std::memory_order_relaxed);
            // Load first and exchange only when it is set: one reactor wins
            // the exchange and throws, so injection produces exactly one
            // quarantine rather than every reactor scheduled in the same
            // window.
            if (server_->fail_worker_for_test_.load(
                    std::memory_order_relaxed) &&
                server_->fail_worker_for_test_.exchange(
                    false, std::memory_order_relaxed)) {
                throw 1;
            }
            // The indexed form targets one reactor deterministically; the
            // exchange resets it so the injection is one-shot.
            const int who = server_->fail_worker_reactor_for_test_.load(
                std::memory_order_relaxed);
            if (who == idx_ &&
                server_->fail_worker_reactor_for_test_.exchange(
                    -1, std::memory_order_relaxed) == idx_) {
                throw 1;
            }
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
                server_->on_reactor_failed(this);
                break;
            }
            for (int i = 0; i < n; ++i) {
                // Events carry the connection id, not the descriptor. A
                // descriptor can be recycled by a close + accept in the same
                // batch, and a stale event would then act on the new
                // connection; an id is never reused, so a retired one simply
                // fails the lookup below.
                const uint64_t tag = events[i].data.u64;
                uint32_t ev = events[i].events;

                // Only drain the wake count here. Queue work still runs once
                // after the whole batch, so a task that creates a connection
                // cannot make a later event in this same batch refer to it.
                if (tag == REACTOR_WAKE_TAG) {
                    drain_wakeup(); continue;
                }

                auto it = conns_.find(tag);
                if (it == conns_.end()) continue;   // retired connection

                // ---- hold a shared_ptr alive across the whole event dispatch
                // ----
                std::shared_ptr<connection> keep_alive = it->second;
                if (!keep_alive) continue;
                connection* c = keep_alive.get();

                // EPOLLERR is the kernel telling us the socket is broken (a
                // reset, typically); draining into it cannot work. Abort so the
                // peer is not left waiting on a stream that will never
                // complete.
                if (ev & EPOLLERR) {
                    close_conn(c, "hup/err", false); continue;
                }
                // EPOLLHUP is not an error by itself. Linux raises it once both
                // directions are shut -- exactly the state a server-initiated
                // graceful close reaches: our shutdown(SHUT_WR), then the
                // peer's FIN. The event normally carries EPOLLIN as well, and
                // that is the path that knows whether the close is parked and
                // should finish orderly (handle_read -> recv() == 0 ->
                // finish_peer_closed). Checking HUP first classified the last
                // step of every such close as an abort: `aborted` incremented,
                // the close reason read "hup/err", and any input the peer had
                // already sent was dropped (measured: clean client close after
                // the server's FIN -> reason "hup/err", aborted=1).
                if (ev & EPOLLIN) {
                    handle_read(c);
                    if (c->sm.closed())
                        continue;
                } else if (ev & EPOLLHUP) {
                    // Nothing readable is left: both directions are shut and
                    // the read path has no EOF to report. End it.
                    close_conn(c, "hup/err", false); continue;
                }
                if (ev & EPOLLOUT) {
                    handle_write(c);
                }
            }
            // Deferred close callbacks run with no per-connection stack frames
            // alive, so a callback may call stop() -- which from there only
            // requests shutdown. Destroying the server is still forbidden (see
            // the class contract): the calling thread is one stop() would have
            // to join.
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
        // Quarantine this reactor. If it was the last live one, this raises the
        // failure flag; otherwise the server keeps serving on the survivors.
        // The lifecycle state stays untouched so a concurrent start() verdict
        // is not overwritten by a worker crash.
        server_->on_reactor_failed(this);
    }
    // Reject submissions even when the loop failed rather than being stopped.
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        running_.store(false, std::memory_order_release);
    }
    // A failed reactor's connections are reset under "reactor failure"; a
    // reactor leaving on request keeps the shutdown reason. The accept thread
    // reaps the failed one afterwards, and its stop() runs cleanup() again on
    // an empty set.
    cleanup(dead_.load(std::memory_order_acquire)
                ? "reactor failure" : "server shutdown");
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
        // Opt-in: with it on, a second process can bind this port and the
        // kernel splits connections between them, so a failed bind no longer
        // tells anyone that a stale instance is still running.
        if (cfg_.reuse_port)
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

// Checks and resolves everything start() needs before it acquires a resource.
// Refusal is just false: start()'s rollback guard owns the lifecycle reset and
// every cleanup, so no branch here can forget one.
inline bool multi_reactor_server::validate_start_config() noexcept {
    if (!reactors_.empty() || accept_thread_.joinable() ||
        cfg_.rbuf_capacity < MAX_FRAME_SIZE ||
        cfg_.wbuf_capacity < FRAME_HEADER_SIZE ||
        cfg_.max_pending_bytes < FRAME_HEADER_SIZE ||
        cfg_.max_connections <= 0 || cfg_.tick_ms <= 0 ||
        cfg_.idle_timeout_ms <= 0 || cfg_.write_budget_bytes == 0 ||
        cfg_.close_linger_ms <= 0 || cfg_.backlog <= 0 ||
        cfg_.frame_assembly_timeout_ms < 0 ||
        cfg_.reactor_stall_timeout_ms < 0 ||
        cfg_.handler_threads < 0 ||
        cfg_.handler_threads > config::k_max_handler_threads ||
        (cfg_.handler_threads > 0 && cfg_.handler_queue_limit == 0)) {
        return false;
    }

    // A healthy reactor's stamped interval is one tick plus the loop body:
    // epoll_wait(tick_ms), then the event batch, pending tasks, the close
    // callback flush and do_tick's scan of every connection. A timeout a hair
    // above tick_ms therefore reports healthy iterations as stalls. The
    // absolute floor keeps a tiny tick from making the watchdog hair-trigger;
    // the operator still has to choose a timeout above the longest expected
    // iteration, because a large connection set or a slow inline handler can
    // make the body arbitrarily long.
    if (cfg_.reactor_stall_timeout_ms > 0) {
        const int64_t floor_ms = std::max<int64_t>(
            100, static_cast<int64_t>(cfg_.tick_ms) * 2);
        if (cfg_.reactor_stall_timeout_ms < floor_ms) {
            std::fprintf(stderr,
                         "[error] reactor_stall_timeout_ms=%d must be 0 or "
                         "at least max(100, 2 * tick_ms=%d) = %" PRId64 "\n",
                         cfg_.reactor_stall_timeout_ms, cfg_.tick_ms,
                         floor_ms);
            return false;
        }
    }

    warn_deadline_granularity();

    // Configuration validation
    if (cfg_.max_pending_bytes > cfg_.wbuf_capacity) {
        cfg_.max_pending_bytes = cfg_.wbuf_capacity;
    }
    // Resolved after the clamp, so a derived high mark never exceeds the
    // ceiling the rest of the reactor will actually enforce.
    {
        const size_t high = cfg_.resolved_read_high_watermark_bytes();
        const size_t low  = cfg_.resolved_read_low_watermark_bytes();
        if (high == 0 || high > cfg_.max_pending_bytes ||
            low == 0 || low >= high) {
            std::fprintf(stderr,
                         "[error] read watermarks must satisfy "
                         "0 < low < high <= max_pending_bytes "
                         "(low=%zu high=%zu max_pending_bytes=%zu)\n",
                         low, high, cfg_.max_pending_bytes);
            return false;
        }
    }
    // Admission budget. The product is resolved after the buffer sizes are
    // known; a budget below one connection's buffers could never admit
    // anything, which is a configuration error rather than a policy.
    if (cfg_.rbuf_capacity >
        static_cast<size_t>(-1) - cfg_.wbuf_capacity) {
        std::fprintf(
            stderr,
            "[error] rbuf_capacity + wbuf_capacity overflows size_t\n");
        return false;
    }
    per_conn_buffer_bytes_ = cfg_.per_connection_buffer_bytes();
    max_buffer_bytes_ = cfg_.resolved_max_total_buffer_bytes();
    if (per_conn_buffer_bytes_ == 0 ||
        max_buffer_bytes_ < per_conn_buffer_bytes_ ||
        cfg_.max_connections_per_ip < 0) {
        std::fprintf(stderr,
                     "[error] admission config invalid: per-connection buffers="
                     "%zu, max_total_buffer_bytes=%zu, "
                     "max_connections_per_ip=%d\n",
                     per_conn_buffer_bytes_, max_buffer_bytes_,
                     cfg_.max_connections_per_ip);
        return false;
    }
    if (max_buffer_bytes_ / per_conn_buffer_bytes_ <
        static_cast<size_t>(cfg_.max_connections)) {
        const size_t admittable = max_buffer_bytes_ / per_conn_buffer_bytes_;
        std::fprintf(stderr,
                     "[warn] max_total_buffer_bytes=%zu admits about %zu of "
                     "max_connections=%d (each reserves %zu bytes)\n",
                     max_buffer_bytes_, admittable, cfg_.max_connections,
                     per_conn_buffer_bytes_);
    }
    if (cfg_.max_connections_per_ip > 0 && !ip_table_) {
        try {
            ip_table_ = std::make_shared<ip_admission>();
        } catch (...) {
            std::fprintf(stderr,
                         "[error] could not allocate the per-IP admission "
                         "table\n");
            return false;
        }
    }
    return true;
}

// Deadline granularity: these timeouts only fire on tick boundaries, so a tick
// coarser than the deadline silently stretches it by up to a full tick. Warn
// rather than refuse -- a coarse tick with long deadlines is a legitimate
// choice; a coarse tick with a 200ms promise is a surprise.
inline void multi_reactor_server::warn_deadline_granularity() noexcept {
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
}

// ---- fd ceiling: raise it if we can, and say how far it goes if we
// cannot ----
// Warning and starting anyway hands operations a server that accepts the first
// thousand connections and then refuses everything without a word, which is
// worse than refusing to boot: it looks healthy until it isn't. Raising the
// soft limit is the one action here that cannot make things worse -- the hard
// ceiling stays where the environment put it -- so try it before reporting.
inline void multi_reactor_server::warn_fd_ceiling() noexcept {
    struct rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        // Descriptors the server needs besides one per connection: the accept
        // epoll + eventfd (2), one epoll + eventfd per reactor, and a small
        // margin. The old flat +64 under-counted from 32 reactors upwards.
        const uintmax_t overhead =
            static_cast<uintmax_t>(
                2 * (1 + cfg_.resolved_reactor_count())) + 16;
        const uintmax_t needed =
            static_cast<uintmax_t>(cfg_.max_connections) + overhead;
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
                cur > overhead ? cur - overhead : 0);
        }
    }
}

// Starts every reactor. A partial set is left in place for the caller's
// rollback guard, which is the single owner of teardown; a reactor that never
// made it into the vector is cleaned by its own destructor.
//
// The whole set is built before any thread starts. The accept thread reaps and
// pick_reactor() scans reactors_, and neither takes a lock; growing the vector
// while they run would be a data race. After this function returns, the vector
// is immutable until stop() has joined the accept thread.
inline bool multi_reactor_server::start_reactors(int n_reactors) noexcept {
    try {
        reactors_.reserve(static_cast<size_t>(n_reactors));
        for (int i = 0; i < n_reactors; ++i)
            reactors_.push_back(std::make_unique<sub_reactor>(i, this));
    } catch (...) {
        return false;   // rollback_start() stops and clears the partial set
    }
    // Count the whole set before any thread exists. A reactor that dies during
    // bring-up must not be mistaken for "the last live one" while its
    // successors are merely not started yet: with per-start increments, a
    // reactor failing before the next was counted would drive the count to 0
    // and fail a start that would have had survivors. A refused start() is
    // subtracted back so the count still means "alive, or about to be".
    live_reactors_.store(reactors_.size(), std::memory_order_release);
    for (auto& r : reactors_) {
        if (!r->start()) {
            live_reactors_.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
    }
    return true;
}

// Startup handshake: returns once the accept thread reports in. Success means
// "the accept loop is armed, no component has failed, and the handler pool is
// up", not "the lifecycle counter is still false". A component that died
// during bring-up raises failed_ instead of rewriting the lifecycle state, so
// the verdict returned here cannot be flipped out from under the caller.
inline bool multi_reactor_server::start_accept_and_handlers() noexcept {
    accepting_.store(true, std::memory_order_release);
    try {
        auto ready = std::make_shared<std::promise<bool>>();
        auto fut = ready->get_future();
        accept_thread_ = std::thread([this, ready]() noexcept {
            accept_loop(*ready);
        });
        if (fut.get()) {
            return !failed_.load(std::memory_order_acquire) &&
                   start_handler_pool();
        }
    } catch (...) {
    }
    return false;
}

// Undo every resource phase start() may have reached. Safe at any point after
// start() publishes `starting`: fields that were never created are -1 or
// empty. The wake makes a parked accept thread leave epoll_wait(...,500)
// immediately; the join still happens before any descriptor is touched, and
// its own guard has normally closed them already.
inline void multi_reactor_server::rollback_start() noexcept {
    accepting_.store(false, std::memory_order_release);
    wake_accept_loop();
    life_.store(life::stopping, std::memory_order_release);
    stop_handler_pool();
    if (accept_thread_.joinable()) accept_thread_.join();
    for (auto& r : reactors_) r->stop();
    reactors_.clear();
    close_accept_descriptors();
    // Keep the allocated slots published: their traffic counters are
    // cumulative and must stay reconcilable with stats(). Mark them retired
    // instead, and clear the per-run fields so a failed start leaves no
    // half-run state behind. The next ensure() reactivates and resets the
    // slots it uses.
    if (reactor_slots_) {
        const size_t exposed =
            reactor_slot_count_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < exposed; ++i) {
            reactor_slot& s = reactor_slots_[i];
            s.connections.store(0, std::memory_order_relaxed);
            s.last_progress_ms.store(0, std::memory_order_relaxed);
            s.dead.store(false, std::memory_order_relaxed);
            s.stalled.store(false, std::memory_order_relaxed);
            s.stalled_at_ms.store(0, std::memory_order_relaxed);
        }
    }
    reactor_run_count_.store(0, std::memory_order_release);
    failed_.store(false, std::memory_order_release);
    life_.store(life::stopped, std::memory_order_release);
}

inline bool multi_reactor_server::start() noexcept {
    if (callback_server_ == this) return false;
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (life_.load(std::memory_order_acquire) != life::stopped) return false;
    life_.store(life::starting, std::memory_order_release);
    failed_.store(false, std::memory_order_release);
    // Armed before validation: every refusal below, including a configuration
    // check that forgets its own cleanup, lands in the same rollback. Fields
    // that do not exist yet are -1 or empty there.
    start_rollback rollback{this};

    if (!validate_start_config()) return false;
    // Resolve the reactor count once for this run and use it for both the
    // slot reset and the reactors themselves. hardware_concurrency() can
    // change between restarts, and two independent resolutions could then
    // disagree. The test override is clamped to the same ceiling.
    int n_reactors = cfg_.resolved_reactor_count();
    const int count_override =
        resolved_reactor_count_for_test_.load(std::memory_order_relaxed);
    if (count_override > 0) {
        n_reactors = count_override > config::k_max_reactors
                         ? config::k_max_reactors : count_override;
    }
    if (!ensure_reactor_slots(static_cast<size_t>(n_reactors)))
        return false;

    warn_fd_ceiling();

    if (!init_listen_socket()) return false;

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
    // Everything above here already fails through the rollback guard and
    // reports false; everything below here is threads that read this value the
    // moment they come into existence. There is nothing left to wait for.
    life_.store(life::running, std::memory_order_release);

    if (!start_reactors(n_reactors)) return false;
    if (!start_accept_and_handlers()) return false;

    rollback.dismiss();
    return true;
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

    // Close the accept thread's descriptors under the mutex that
    // wake_accept_loop() holds. Reactors are still running here: one that
    // fails during shutdown calls wake_accept_loop(), and closing these
    // descriptor numbers around that write would be a race on a recycled fd.
    close_accept_descriptors();

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
    // close_connection() queues a close_task every time it succeeds. Calling it
    // for every still-open id on every 10 ms pass made a connection that was
    // slow to drain collect one close request per pass -- on a large server
    // enough of them to fill the reactor queues, where they competed with the
    // drain/park continuations the close itself depends on. Ask once per id;
    // only a request the queue actually refused is retried. The connection's
    // own linger deadline bounds the rest.
    std::unordered_set<uint64_t> requested;
    while (now_ms() < deadline) {
        std::vector<uint64_t> ids;
        try {
            ids = connection_ids();
        } catch (...) {
            break;  // Could not snapshot; fall through to stop() below.
        }
        if (ids.empty() && admitted_.load(std::memory_order_acquire) == 0) {
            break;
        }
        for (uint64_t id : ids) {
            if (requested.count(id) != 0) continue;
            if (!close_connection(id)) continue;
            // Remembering the id must not turn into std::terminate: this
            // function is noexcept, and an allocation failure here is not a
            // reason to kill a server that is already shutting down. Failing to
            // record it only means the next pass asks this one again.
            try {
                requested.insert(id);
            } catch (...) {
            }
        }
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
    // Checked before the no-pool return, so arming the failpoint always refuses
    // and is always consumed, whatever handler_threads is.
    if (fail_handler_pool_for_test_.load(std::memory_order_relaxed)) {
        fail_handler_pool_for_test_.store(false, std::memory_order_relaxed);
        return false;
    }
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

inline void multi_reactor_server::on_reactor_failed(
        sub_reactor* r) noexcept {
    // Order matters: publish the quarantine flag, then count the failure, then
    // decide whether this was the last live reactor. The decrement's return
    // value is the whole decision -- resetting a flag and scanning the others
    // would be a store-then-scan over separate atomics, which a weak memory
    // model is free to reorder into "nobody was last".
    r->dead_.store(true, std::memory_order_release);
    if (r->slot_) {
        // A dead slot is never sampled again, so a stall flag left set here
        // would outlive the watchdog's own definition of it and show up in
        // reactor_stats() as "stalled but not routed to" on a reactor that
        // cannot be routed to at all. dead is the stronger state; drop the
        // weaker one with it.
        r->slot_->stalled.store(false, std::memory_order_relaxed);
        r->slot_->stalled_at_ms.store(0, std::memory_order_relaxed);
        r->slot_->dead.store(true, std::memory_order_release);
    }
    stats_.reactor_failures.fetch_add(1, std::memory_order_relaxed);
    if (live_reactors_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        fail();
    // The accept thread owns the reaper, and its own epoll does not see a
    // reactor's death; wake it so the dead reactor is joined promptly instead
    // of after its 500 ms wait.
    wake_accept_loop();
}

inline void multi_reactor_server::reap_dead_reactors() noexcept {
    for (auto& r : reactors_) {
        if (!r->dead_.load(std::memory_order_acquire)) continue;
        // The CAS is the claim on the join. stop() runs the teardown and
        // closes this reactor's epoll fd and eventfd; the reactor's own loop
        // already ran cleanup() before exiting, so no close callback is
        // delivered from the accept thread.
        if (r->reaped_.exchange(true, std::memory_order_acq_rel)) continue;
        r->stop();
    }
}

inline sub_reactor* multi_reactor_server::pick_reactor() noexcept {
    const size_t n = reactors_.size();
    if (n == 0) return nullptr;
    // Step past quarantined reactors. A dead reactor keeps its slot in the
    // vector until stop() clears it, so scanning at most n entries is what
    // distinguishes "every reactor is dead" (null) from a live one behind the
    // cursor. This is not the last-live decision -- that is live_reactors_.
    //
    // A stalled reactor is skipped for the same reason a dead one is, one step
    // weaker: it still owns its connections and will serve queued work for them
    // when it resumes, but a *new* connection handed to it waits in a queue
    // nobody is draining, and the peer sees a live TCP connection that never
    // answers. It cannot be told apart from a slow server, so the connection is
    // routed to a reactor that is known to be running instead.
    //
    // If every live reactor is stalled the bad hand-off is taken anyway: one
    // queue that drains late still beats refusing every connection, and a
    // stalled reactor is exactly what a server under a long inline handler
    // looks like. `skips` therefore counts rerouted connections only, and
    // stays put in that fallback.
    sub_reactor* fallback = nullptr;
    bool skipped = false;
    for (size_t tries = 0; tries < n; ++tries) {
        const size_t i =
            next_reactor_.fetch_add(1, std::memory_order_relaxed) % n;
        sub_reactor* r = reactors_[i].get();
        if (r->dead_.load(std::memory_order_acquire)) continue;
        // Relaxed: this flag publishes nothing but itself, and a stale read
        // costs one connection routed to a stalled reactor -- the situation
        // before it existed. A `dead` read, by contrast, must be acquire
        // because the reactor's teardown is published through it.
        if (r->slot_ &&
            r->slot_->stalled.load(std::memory_order_relaxed)) {
            if (!fallback) fallback = r;
            skipped = true;
            continue;
        }
        if (skipped)
            stats_.reactor_stall_skips.fetch_add(
                1, std::memory_order_relaxed);
        return r;
    }
    return fallback;
}

inline bool multi_reactor_server::ensure_reactor_slots(size_t n) noexcept {
    // Allocate once at the maximum count: hardware_concurrency() can change
    // between restarts, and a later run must not index past the array.
    const size_t capacity = static_cast<size_t>(config::k_max_reactors);
    if (!reactor_slots_) {
        try {
            reactor_slots_.reset(new reactor_slot[capacity]());
        } catch (...) {
            std::fprintf(stderr,
                         "[error] could not allocate per-reactor stats\n");
            return false;
        }
        published_slots_.store(reactor_slots_.get(),
                               std::memory_order_release);
    }
    // Publish every slot that has ever been used. When a later run resolves
    // fewer reactors, the slots above the new count stay visible as retired:
    // their traffic counters are cumulative and never reset, so hiding them
    // would break the sum invariant against server_stats(). Traffic counters
    // are cumulative since construction; only per-run state is reset, and it
    // is reset for retired slots as well.
    size_t exposed = reactor_slot_count_.load(std::memory_order_relaxed);
    if (n > exposed) exposed = n;
    reactor_slot_count_.store(exposed, std::memory_order_release);
    reactor_run_count_.store(n, std::memory_order_release);
    for (size_t i = 0; i < exposed; ++i) {
        reactor_slot& s = reactor_slots_[i];
        s.connections.store(0, std::memory_order_relaxed);
        s.last_progress_ms.store(0, std::memory_order_relaxed);
        s.dead.store(false, std::memory_order_relaxed);
        s.stalled.store(false, std::memory_order_relaxed);
        s.stalled_at_ms.store(0, std::memory_order_relaxed);
    }
    return true;
}

inline std::vector<multi_reactor_server::reactor_stats_view>
multi_reactor_server::reactor_stats() const {
    std::vector<reactor_stats_view> out;
    const reactor_slot* slots =
        published_slots_.load(std::memory_order_acquire);
    if (!slots) return out;
    const size_t n = reactor_slot_count_.load(std::memory_order_acquire);
    const size_t active =
        reactor_run_count_.load(std::memory_order_acquire);
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        const reactor_slot& s = slots[i];
        reactor_stats_view v;
        v.frames_recv = s.frames_recv.load(std::memory_order_relaxed);
        v.frames_sent = s.frames_sent.load(std::memory_order_relaxed);
        v.bytes_recv = s.bytes_recv.load(std::memory_order_relaxed);
        v.bytes_sent = s.bytes_sent.load(std::memory_order_relaxed);
        v.connections = s.connections.load(std::memory_order_relaxed);
        v.last_progress_ms =
            s.last_progress_ms.load(std::memory_order_relaxed);
        v.dead = s.dead.load(std::memory_order_acquire);
        v.stalled = s.stalled.load(std::memory_order_relaxed);
        v.retired = i >= active;
        out.push_back(v);
    }
    return out;
}

inline void multi_reactor_server::check_reactor_stalls() noexcept {
    // Only a running server has live reactors. During failure teardown the
    // survivors leave through the normal is_running() check without a final
    // stamp; evaluating their stale timestamps would report healthy reactors
    // as stalled for the one pass the accept thread still makes.
    if (!is_running()) return;
    const int64_t timeout = cfg_.reactor_stall_timeout_ms;
    if (timeout <= 0) return;
    reactor_slot* slots = published_slots_.load(std::memory_order_acquire);
    if (!slots) return;
    const size_t active =
        reactor_run_count_.load(std::memory_order_acquire);
    const int64_t now = now_ms();
    for (size_t i = 0; i < active; ++i) {
        reactor_slot& s = slots[i];
        if (s.dead.load(std::memory_order_relaxed)) continue;
        const int64_t last =
            s.last_progress_ms.load(std::memory_order_relaxed);
        if (last == 0) continue;
        const bool stalled = s.stalled.load(std::memory_order_relaxed);
        if (now - last > timeout) {
            if (s.stalled.exchange(true, std::memory_order_relaxed))
                continue;   // still the same episode: already counted, logged
            s.stalled_at_ms.store(now, std::memory_order_relaxed);
            stats_.reactor_stalls.fetch_add(1, std::memory_order_relaxed);
            std::fprintf(stderr,
                "[warn] reactor %zu made no progress for %" PRId64 " ms "
                "(reactor_stall_timeout_ms=%d); new connections are routed "
                "elsewhere until it recovers\n",
                i, now - last, cfg_.reactor_stall_timeout_ms);
        } else if (stalled) {
            // Hysteresis, and the reason there is a state to leave at all. The
            // entry condition is a full timeout of silence; the exit is two
            // conditions: the reactor looks fresh by half a window, and a full
            // window has passed since the stall was declared. Without the
            // second one a reactor that blocks for just over the timeout every
            // so often would be re-admitted the moment it reopens its eyes,
            // then dropped again -- a routing flap, and one log line and one
            // counter increment per flap.
            //
            // The cost of the dwell is that a reactor which recovers
            // immediately after being flagged takes up to timeout longer to
            // receive new connections. That is the trade: capacity returns
            // late rather than repeatedly.
            const int64_t marked =
                s.stalled_at_ms.load(std::memory_order_relaxed);
            if (now - last <= timeout / 2 && marked != 0 &&
                now - marked >= timeout) {
                s.stalled.store(false, std::memory_order_relaxed);
            }
        }
    }
}

inline int multi_reactor_server::accept_wait_ms() const noexcept {
    const int timeout = cfg_.reactor_stall_timeout_ms;
    if (timeout <= 0) return 500;
    // Validation guarantees timeout >= 100, so half is at least 50 ms. The
    // max() is belt and braces against a future change to that floor.
    return std::max(10, std::min(500, timeout / 2));
}

// Opens everything the accept thread owns. The caller's accept_cleanup closes
// whatever was opened before a failure, so this may return early without
// unwinding by hand.
inline bool multi_reactor_server::open_accept_descriptors() noexcept {
    std::lock_guard<std::mutex> descriptors(accept_fd_mutex_);
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) return false;
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) return false;
    // Held back for EMFILE. Failure is not fatal: without it the accept loop
    // degrades to the old sleep-and-retry, which is worse but not wrong, and
    // salvage_descriptor_exhaustion checks for the -1.
    spare_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_;
    if (fail_epoll_add_for_test_.load(std::memory_order_relaxed)) {
        fail_epoll_add_for_test_.store(false, std::memory_order_relaxed);
        return false;
    }
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) != 0)
        return false;
    ev.data.fd = wake_fd_;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) == 0;
}

inline void multi_reactor_server::close_accept_descriptors() noexcept {
    // The same mutex protects stop's wake write, so a descriptor number cannot
    // be recycled between its check and that write.
    std::lock_guard<std::mutex> descriptors(accept_fd_mutex_);
    if (spare_fd_ >= 0) {
        ::close(spare_fd_); spare_fd_ = -1;
    }
    if (wake_fd_ >= 0) {
        ::close(wake_fd_); wake_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_); epoll_fd_ = -1;
    }
    close_listen_socket();
}

// The rescue returns to the accept round when the victim accept was
// interrupted or the queued peer aborted: neither says the backlog was empty.
// Any other failure (notably EAGAIN) means nothing was waiting, so the round
// should stop rather than spend its remaining budget.
inline bool accept_salvage_retries(int victim_errno) noexcept {
    return victim_errno == EINTR || victim_errno == ECONNABORTED;
}

// EMFILE and ENFILE, answered without giving up the accept round. progress
// means a client was turned away or the round can retry immediately; backoff
// means the table is full and nothing local can change that; stop means the
// wake was stale and the caller should return to epoll_wait.
inline multi_reactor_server::accept_salvage
multi_reactor_server::salvage_descriptor_exhaustion(
        int accept_errno) noexcept {
    stats_.accept_errors.fetch_add(1, std::memory_order_relaxed);
    // ENFILE is the system-wide table. Closing our own fd cannot free a slot
    // there, so it keeps the backoff.
    if (accept_errno == ENFILE) {
        stats_.fd_exhausted_system.fetch_add(1, std::memory_order_relaxed);
        return accept_salvage::backoff;
    }
    // Another thread can take the slot freed by closing the spare, so the
    // reopen at the end of this block can fail. Try again here on a later
    // EMFILE rather than leaving the rescue disabled until the next start().
    if (spare_fd_ < 0)
        spare_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (spare_fd_ < 0) return accept_salvage::backoff;

    // Process-level exhaustion can be answered: spend the reserved descriptor
    // on the connection the kernel is holding, reset it, and take a descriptor
    // back. The peer learns in microseconds instead of waiting in the backlog
    // for a slot that -- on a server that owns the machine -- never comes.
    ::close(spare_fd_);
    spare_fd_ = -1;
    const int victim = ::accept4(listen_fd_, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
    const int victim_errno = errno;
    if (victim >= 0) {
        // Never served, so never FIN: an orderly close would tell the peer the
        // server hung up on purpose, which is a different lie.
        struct linger lg{};
        lg.l_onoff  = 1;
        lg.l_linger = 0;
        ::setsockopt(victim, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        ::close(victim);
        stats_.rejected.fetch_add(1, std::memory_order_relaxed);
        stats_.fd_exhausted_drops.fetch_add(1, std::memory_order_relaxed);
    }
    spare_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    const int64_t now = now_ms();
    if (now - last_fd_log_ms_ >= 1000) {
        last_fd_log_ms_ = now;
        std::fprintf(stderr,
            "[warn] out of descriptors: dropped %ju connection(s) so far; "
            "raise RLIMIT_NOFILE or lower max_connections\n",
            static_cast<uintmax_t>(stats_.fd_exhausted_drops.load()));
    }
    // accept4 allocates the descriptor before it looks at the queue, so with
    // the table full it reports EMFILE even when nothing is waiting. EAGAIN
    // here is that stale wake: the spare was spent and replaced and bought no
    // progress, so the caller returns to epoll_wait instead of spending the
    // rest of this round's 64. EINTR and ECONNABORTED describe the attempt,
    // not an empty queue, so the round keeps its budget and retries.
    if (victim < 0 && !accept_salvage_retries(victim_errno))
        return accept_salvage::stop;
    return accept_salvage::progress;
}

// Admission, options, peer keys and hand-off for one accepted socket. The
// guard owns cfd on every path that does not hand it to a reactor.
inline void multi_reactor_server::admit_accept(
        int cfd, const struct sockaddr_storage& ss,
        socklen_t slen) noexcept {
    fd_guard cfd_guard(cfd);
    bool reject = false;
    bool over_budget = false;
    if (admitted_.load() >= static_cast<size_t>(cfg_.max_connections))
        reject = true;
    // Advisory like the count above: this load races the reservations by
    // definition, and the ceiling is a bound on the steady state, not a hard
    // barrier.
    const size_t committed =
        stats_.buffer_bytes.load(std::memory_order_relaxed);
    over_budget = committed > max_buffer_bytes_ ||
                  per_conn_buffer_bytes_ > max_buffer_bytes_ - committed;
    if (over_budget) reject = true;
    if (reject) {
        stats_.rejected.fetch_add(1, std::memory_order_relaxed);
        if (over_budget)
            stats_.buffer_budget_rejections.fetch_add(
                1, std::memory_order_relaxed);
        return;   // cfd_guard closes it
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
        std::string peer, ip_key;
        peer_strings_from_sockaddr(ss, slen, &peer, &ip_key);

        sub_reactor* r = pick_reactor();
        if (!r) {
            // Every reactor is quarantined. The fd guard closes the socket;
            // count the refusal so a dead pool is visible in rejected instead
            // of looking like an accept that silently vanished.
            stats_.rejected.fetch_add(1, std::memory_order_relaxed);
            return;   // cfd_guard closes it
        }

        uint64_t id = next_conn_id_.fetch_add(
            1, std::memory_order_relaxed);
        // REACTOR_WAKE_TAG is 0. Skipping the wrap keeps that sentinel
        // unambiguous; the counter has to pass through 2^64 ids to get here,
        // so this is a formality that costs nothing.
        if (id == REACTOR_WAKE_TAG)
            id = next_conn_id_.fetch_add(1, std::memory_order_relaxed);
        // Submission consumes ownership on both success and rejection.
        // accepted is NOT bumped here: queueing a task is not a connection.
        // The reactor bumps it once the socket is registered in both maps, so
        // a task that fails during registration counts as rejected only.
        auto admission = std::make_shared<admission_ticket>(
            admitted_, stats_.buffer_bytes, per_conn_buffer_bytes_,
            ip_table_, std::move(ip_key));
        if (!admission->acquire_ip(cfg_.max_connections_per_ip)) {
            // The ticket's destructor gives back the global count and byte
            // reservation; no per-IP slot was taken.
            stats_.rejected.fetch_add(1, std::memory_order_relaxed);
            stats_.ip_rejections.fetch_add(1, std::memory_order_relaxed);
            return;   // cfd_guard closes it
        }
        if (!r->submit_new_connection(std::move(cfd_guard), std::move(peer),
                                      id, std::move(admission))) {
            stats_.rejected.fetch_add(1, std::memory_order_relaxed);
        }
    } catch (...) {
        // Before handoff, cfd_guard still owns the accepted socket.
        stats_.rejected.fetch_add(1, std::memory_order_relaxed);
    }
}

inline void multi_reactor_server::accept_loop(
        std::promise<bool>& ready) noexcept {
    // One owner for every descriptor this thread opens. Constructed first, so
    // a failed setup, an exception from the accept path, and the normal loop
    // exit all run the same close block. Its absence was exactly the early
    // return that used to leak spare_fd_.
    accept_cleanup cleanup{this};
    try {
    const bool ok = open_accept_descriptors();
    try {
        ready.set_value(ok);
    } catch (...) {}
    // Descriptor setup failure is the accept loop's own failure: raise the
    // shared flag instead of forcing the whole lifecycle down, so start()
    // reports false through its normal recovery path rather than racing on the
    // state. Returning is safe now: the guard above closes the spare as well
    // as the descriptors start() knows about.
    if (!ok) {
        fail(); return;
    }

    std::array<struct epoll_event, 64> events{};
    while (accepting_.load(std::memory_order_acquire) && is_running()) {
        // Reap before parking in epoll_wait. A failed reactor wakes this thread
        // through wake_accept_loop(), so at most one iteration passes between
        // its death and its join.
        reap_dead_reactors();
        if (fail_accept_for_test_.load(std::memory_order_relaxed)) {
            fail_accept_for_test_.store(false, std::memory_order_relaxed);
            throw 1;
        }
        int n = ::epoll_wait(epoll_fd_, events.data(),
                             static_cast<int>(events.size()),
                             accept_wait_ms());
        if (n < 0) {
            if (errno == EINTR) {
                // A signal must not skip the only watchdog sample. Without
                // this, a steady signal stream starves it entirely.
                check_reactor_stalls();
                continue;
            }
            stats_.reactor_failures.fetch_add(1, std::memory_order_relaxed);
            fail();
            break;
        }
        check_reactor_stalls();
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
                    const int accept_errno = errno;
                    // Out of descriptors is not an error to swallow and it is
                    // not one to spin on either: the listening socket stays
                    // readable, so returning to epoll_wait immediately means
                    // burning a core rejecting nothing. It is also the one
                    // accept failure that can be predicted at startup and
                    // therefore should be countable -- without a counter the
                    // only symptom is "it stopped accepting and said nothing".
                    if (accept_errno == EMFILE || accept_errno == ENFILE) {
                        const accept_salvage action =
                            salvage_descriptor_exhaustion(accept_errno);
                        if (action != accept_salvage::stop) {
                            if (action == accept_salvage::backoff)
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(1));
                            continue;
                        }
                    }
                    break;
                }
                admit_accept(cfd, ss, slen);
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
}

inline bool multi_reactor_server::on_connection_opened(
    uint64_t id, std::shared_ptr<connection> c) noexcept {
    try {
        conn_shard& shard = conn_shards_[conn_shard_for(id)];
        std::unique_lock<std::shared_mutex> lk(shard.mutex);
        shard.map[id] = std::move(c);
        return true;
    } catch (...) {
        // No counter here either. queue_drops counts submissions refused at
        // enqueue, and this failure happens after the task was accepted -- the
        // false return is what the reactor counts as rejected, which is the one
        // question this answer can support.
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
    if (c->sm.refuses_sends())
        return send_result::closed;

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
    // the immediate hand-off; the close itself is no longer lost -- see below.)
    if (!c->owner) {
        stats_.close_requests_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    // Count first, then queue. A send_to() running on the owner reactor must
    // fall back to the queued path from the moment the close starts being
    // queued, not from the moment the caller returns: otherwise a handler
    // already in flight can pass the fast-path check while the close task is
    // being appended, and write ahead of it (R9-4).
    c->sm.note_close_request();
    if (!c->owner->submit_close(conn_id)) {
        // The queue refused the task, but the request is still honoured:
        // close_wanted gives it to do_tick's sweep, which performs the same
        // drain-then-close the task would have. The count stays incremented so
        // a send racing this path keeps taking the queued branch, and the
        // refusal is still visible as close_requests_dropped.
        //
        // Returns true, not false: the close is going to happen, and reporting
        // failure here made callers retry a request that was already
        // guaranteed -- stop_gracefully() did exactly that, bumping
        // close_requests once per 10 ms pass for every connection whose task
        // the queue had refused, and competing with the drain it was waiting
        // for.
        c->sm.post_close_wanted();
        stats_.close_requests_dropped.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
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
