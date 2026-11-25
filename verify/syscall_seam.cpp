// verify/syscall_seam.cpp -- failure paths as deterministic unit tests.
//
// Build (the policy must be selected before any include of tcp-server-mt.h):
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread
//       -Iinclude -Iverify -DTCP_SERVER_SYSCALL_POLICY=industrial::seam_policy
//       verify/syscall_seam.cpp -o syscall_seam
//
// Each case arms one one-shot errno in the policy, drives the corresponding
// reactor path over a real loopback socket, and asserts the observable result:
// a counter, an RST where FIN would otherwise appear, or the server surviving
// a failure that used to need a lowered rlimit to reproduce.

#define TCP_SERVER_SYSCALL_POLICY industrial::seam_policy
#include "syscall_seam_policy.h"
#include "harness.h"

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using industrial::frame_type;
using industrial::seam_policy;
using industrial::server_stats;

namespace {

industrial::multi_reactor_server::config base_config() {
    industrial::multi_reactor_server::config cfg;
    cfg.port = 0;
    cfg.num_reactors = 2;
    return cfg;
}

bool wait_for(int timeout_ms, const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

uint64_t counter(const std::atomic<uint64_t>& a) {
    return a.load(std::memory_order_relaxed);
}

// A reactor parked in the message handler cannot drain its task queue, which
// is what makes the wake count deterministic. The gate publishes the incoming
// message's connection id under the mutex the handler waits on.
struct blocked_handler {
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    uint64_t conn_id = 0;

    bool wait_for_entry(int timeout_ms) {
        std::unique_lock<std::mutex> lk(m);
        return cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                           [this] { return entered; });
    }

    void release_once() {
        {
            std::lock_guard<std::mutex> lk(m);
            release = true;
        }
        cv.notify_all();
    }
};

struct batch_result {
    int accepted;
    uint64_t wakes;
};

batch_result queue_batch(harness::fixture& f, uint64_t id, uint8_t byte,
                         int count) {
    const uint64_t before = counter(seam_policy::wake_writes);
    int accepted = 0;
    for (int i = 0; i < count; ++i) {
        if (f.server().send_to(id, &byte, 1) == industrial::send_result::ok) {
            ++accepted;
        }
    }
    return {accepted, counter(seam_policy::wake_writes) - before};
}

int count_frames(harness::peer& p, uint8_t byte, int count) {
    int received = 0;
    for (int i = 0; i < count; ++i) {
        harness::frame out;
        if (!p.expect_frame(&out, 2000)) break;
        if (out.type == frame_type::DATA && out.payload.size() == 1 &&
            out.payload[0] == byte) {
            ++received;
        }
    }
    return received;
}

void case_recv_error() {
    std::printf("A. recv error aborts the connection deterministically\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");
    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");

    const uint64_t before_aborted = counter(f.stats().aborted);
    const uint64_t before_calls = counter(seam_policy::recv_calls);
    // A single ECONNRESET on the next recv. The reactor must abort instead of
    // treating the error as EOF (which would send a FIN) or retrying forever.
    seam_policy::recv_errno.store(ECONNRESET, std::memory_order_release);
    const uint8_t one = 0x42;
    (void)p.send_raw(&one, 1);

    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().aborted) > before_aborted;
    }), "recv error counted as an abort");
    harness::expect(counter(seam_policy::recv_calls) > before_calls,
                    "the seam actually saw the recv");
    harness::expect(seam_policy::recv_errno.load() == 0,
                    "the injected errno was consumed once");
    harness::expect(!!p.expect_close(harness::close_kind::rst),
                    "the peer saw a reset, not a FIN");
}

void case_sendmsg_epipe() {
    std::printf("B. sendmsg EPIPE counts and aborts without a real RST window\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");
    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");

    // Fire a request first so the reactor has a reply queued and is about to
    // drain it. Armed before the send because the write side runs on the
    // reactor thread and there is no deterministic checkpoint after it.
    const uint64_t before_epipe = counter(f.stats().epipe_writes);
    const uint64_t before_aborted = counter(f.stats().aborted);
    seam_policy::sendmsg_errno.store(EPIPE, std::memory_order_release);
    (void)p.send(frame_type::PING, nullptr, 0);

    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().epipe_writes) > before_epipe;
    }), "EPIPE counted");
    harness::expect(counter(f.stats().aborted) > before_aborted,
                    "EPIPE aborted instead of spinning on EPOLLOUT");
    harness::expect(!!p.expect_close(harness::close_kind::rst),
                    "the peer saw a reset");
}

void case_sendmsg_short_write() {
    std::printf("C. a short sendmsg still delivers the whole frame\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");
    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");

    // One byte on the first sendmsg, then the real syscall. The drain loop
    // must consume the partial write and continue rather than losing bytes or
    // counting the frame as sent at the wrong boundary.
    seam_policy::sendmsg_short_bytes.store(1, std::memory_order_release);
    // PING produces a PONG from the server itself; no application handler is
    // involved, so the only write in the test is the one being injected.
    (void)p.send(frame_type::PING, nullptr, 0);

    harness::frame out;
    harness::expect(!!p.expect_frame(&out), "the full reply arrived");
    harness::expect(out.type == frame_type::PONG,
                    "the PONG was not truncated by the short write");
    harness::expect(counter(f.stats().frames_sent) >= 1,
                    "frame accounting survived the short write");
}

void case_shutdown_failure() {
    std::printf("D. shutdown failure ends a parked close with a reset\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");
    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");

    const uint64_t before_calls = counter(seam_policy::shutdown_calls);
    const uint64_t before_aborted = counter(f.stats().aborted);
    // Graceful close needs a peer FIN before the server sends its own: the FSM
    // sends FIN only from closing, and a public close parks first. Half-close
    // the peer's write side so handle_read sees EOF while the close is parked.
    seam_policy::shutdown_errno.store(ENOTCONN, std::memory_order_release);
    (void)f.server().stop_gracefully(2000);

    harness::expect(wait_for(2000, [&] {
        return counter(seam_policy::shutdown_calls) > before_calls;
    }), "shutdown was attempted through the seam");
    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().aborted) > before_aborted;
    }), "failed shutdown aborted the connection");
    harness::expect(!!p.expect_close(harness::close_kind::rst),
                    "the peer saw a reset, not an unearned FIN");
}

void case_epoll_ctl_mod_failure() {
    std::printf("E. epoll_ctl MOD failure during a park is counted and aborts\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");
    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");

    const uint64_t before_errors = counter(f.stats().epoll_ctl_errors);
    const uint64_t before_aborted = counter(f.stats().aborted);
    // Target only EPOLL_CTL_MOD: setup and rejection use ADD and must succeed,
    // otherwise the failure would be indistinguishable from a start() failure.
    seam_policy::epoll_ctl_op.store(EPOLL_CTL_MOD, std::memory_order_release);
    seam_policy::epoll_ctl_errno.store(EPERM, std::memory_order_release);
    (void)p.send(frame_type::PING, nullptr, 0);

    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().epoll_ctl_errors) > before_errors;
    }), "epoll_ctl error was counted");
    harness::expect(counter(f.stats().aborted) > before_aborted,
                    "the connection was aborted on interest failure");
    harness::expect(seam_policy::epoll_ctl_errno.load() == 0,
                    "the MOD fault was consumed once");
}

void case_wakeup_coalescing() {
    std::printf("G. a queued batch gets one reactor wake, not one per task\n");
    seam_policy::reset();

    // Block the only reactor in its handler so the queue cannot drain.
    blocked_handler gate;
    auto cfg = base_config();
    cfg.num_reactors = 1;
    harness::fixture f(cfg, [&](uint64_t id, const uint8_t*, size_t) {
        std::unique_lock<std::mutex> lk(gate.m);
        gate.conn_id = id;
        gate.entered = true;
        gate.cv.notify_all();
        gate.cv.wait(lk, [&] { return gate.release; });
    });
    harness::expect(f.started(), "server started");

    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");
    const uint8_t byte = 0x51;
    (void)p.send(frame_type::DATA, &byte, 1);

    if (!gate.wait_for_entry(2000)) {
        gate.release_once();
        harness::expect(false, "handler reached the blocking checkpoint");
        return;
    }

    constexpr int kTasks = 512;
    const batch_result batch = queue_batch(f, gate.conn_id, byte, kTasks);
    gate.release_once();

    harness::expect(batch.accepted == kTasks, "every queued send was accepted");
    harness::expect(batch.wakes <= 1,
                    "512 queued tasks caused at most one eventfd write",
                    "observed " + std::to_string(batch.wakes));
    harness::expect(batch.wakes >= 1,
                    "the empty-to-nonempty transition still woke the reactor");

    const int received = count_frames(p, byte, kTasks);
    harness::expect(received == kTasks,
                    "all coalesced tasks were drained in order",
                    "received " + std::to_string(received) +
                        ", frames_sent " +
                        std::to_string(counter(f.stats().frames_sent)) +
                        ", queue_drops " +
                        std::to_string(counter(f.stats().queue_drops)));
}

void case_accept_emfile() {
    std::printf("F. accept4 EMFILE is counted without a lowered rlimit\n");
    seam_policy::reset();
    harness::fixture f(base_config());
    harness::expect(f.started(), "server started");

    const uint64_t before_errors = counter(f.stats().accept_errors);
    const uint64_t before_drops = counter(f.stats().fd_exhausted_drops);
    // A pending connection has to exist for the kernel to hand one to accept4;
    // connect() returns before the accept thread consumes it, so the fault is
    // armed first and the salvage path accepts the victim for real.
    seam_policy::accept4_errno.store(EMFILE, std::memory_order_release);
    harness::peer p;
    (void)p.connect(f.port());

    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().accept_errors) > before_errors;
    }), "accept error was counted");
    harness::expect(wait_for(2000, [&] {
        return counter(seam_policy::accept4_calls) >= 2;
    }), "both accept4 calls went through the seam");
    // Wait for the effect, not the call. accept4_calls is bumped on entry to
    // the salvage's accept4, but fd_exhausted_drops only after that call
    // returns and the victim is closed. Under TSan those two points are far
    // enough apart that a bare read here can see 0 and report a failure the
    // server did not make.
    harness::expect(wait_for(2000, [&] {
        return counter(f.stats().fd_exhausted_drops) > before_drops;
    }), "the queued victim was reset once, not left in the backlog");
    harness::expect(f.server().is_running(), "the server survived EMFILE");

    // One-shot: the next peer must get in normally, proving salvage restored
    // the reserve and did not leave the accept loop wedged.
    harness::peer q;
    harness::expect(!!q.connect(f.port()), "a later connection is accepted");
}

void case_short_write_iovec_bounds() {
    std::printf("H. a short write can span more than two iovecs\n");
    seam_policy::reset();
    int sv[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        harness::expect(false, "socketpair created");
        return;
    }

    uint8_t a[2] = {1, 2};
    uint8_t b[2] = {3, 4};
    uint8_t c[2] = {5, 6};
    struct iovec vec[3] = {{a, sizeof(a)}, {b, sizeof(b)}, {c, sizeof(c)}};
    struct msghdr msg{};
    msg.msg_iov = vec;
    msg.msg_iovlen = 3;

    // Five bytes cross both iovec boundaries and touch the third segment.
    // A two-element array in the policy would be indexed out of bounds here.
    seam_policy::sendmsg_short_bytes.store(5, std::memory_order_release);
    const ssize_t sent = seam_policy::sendmsg(sv[0], &msg, MSG_NOSIGNAL);
    harness::expect(sent == 5,
                    "the injected short write sent exactly five bytes",
                    "sent " + std::to_string(sent));

    uint8_t got[6] = {};
    const ssize_t nread = ::recv(sv[1], got, sizeof(got), MSG_DONTWAIT);
    harness::expect(nread == 5, "only the injected bytes reached the peer",
                    "read " + std::to_string(nread));
    harness::expect(got[0] == 1 && got[1] == 2 && got[2] == 3 &&
                        got[3] == 4 && got[4] == 5,
                    "the bytes crossed both boundaries in order");

    ::close(sv[0]);
    ::close(sv[1]);
}

}  // namespace

int main() {
    std::printf("== syscall seam ==\n");
    seam_policy::reset();
    case_recv_error();
    case_sendmsg_epipe();
    case_sendmsg_short_write();
    case_short_write_iovec_bounds();
    case_shutdown_failure();
    case_epoll_ctl_mod_failure();
    case_accept_emfile();
    case_wakeup_coalescing();
    std::printf("== syscall seam end ==\n");
    return harness::finish();
}
