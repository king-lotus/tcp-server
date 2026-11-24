// verify/fault_injection.cpp -- what a worker dying is supposed to look like.
//
// The fault switches these cases flip are private members of the server,
// reachable through industrial::test_access (see harness.h). A switch nobody
// flips is no more a test than a commented-out assert.
//
// The exception switches (A and B) raise the same question: what recovery
// means for a thread that left its loop through an exception rather than a
// request.
//
//   A. fail_worker_for_test -- exactly one reactor throws out of loop(), and it
//      must be quarantined rather than fatal: reactor_failures moves once,
//      is_running() stays true, the survivor answers, reactor_stats() reports
//      the dead slot, a second failure kills the last reactor, and the server
//      still stops and restarts cleanly.
//   B. fail_accept_for_test -- the accept loop's exception is still fatal,
//      plus: the listening socket it owned must go away, or the port is held
//      by a server that is not listening.
//   C. Injecting across repeated restarts -- anything that claims a failed worker
//      is recoverable has to survive being restarted after one died, because that
//      is the only shape "recoverable" can take that anyone would rely on.
//   D. close_linger_ms <= 0 must be refused at start(): do_tick only sweeps a parked
//      close when linger > 0, so a non-positive value turns every peer that never
//      answers our FIN into a permanent fd + admission leak. Same for backlog <= 0:
//      listen() takes the value as given, and a zero-length accept queue looks
//      exactly like a server that randomly refuses connections under load.
//   E. a message handler that throws is the application's bug, not a reactor
//      failure. The throw happens inside parse_frames(), which is noexcept, so
//      an uncaught one is std::terminate() -- every connection, not just the
//      one whose message it was. The open state handler already absorbs the
//      same violation (R8-2); this records that the message handler does too
//      (R9-2), and that the connection keeps dispatching afterwards.
//   F. fail_close_enqueue_for_test -- a close request the queue refused must
//      still close the connection. close_wanted records it and do_tick's sweep
//      performs the drain-then-close within one tick, so a full queue delays
//      the close instead of losing it.
//   G. fail_epoll_add_for_test -- accept setup failing after the spare
//      descriptor has been opened must release it too. start()'s recovery
//      closes only the descriptors it knows about, and a failed start leaves
//      the lifecycle at `stopped`, so stop() will not clean up afterwards.
//   H. fail_handler_pool_for_test -- the deepest start failure: the listening
//      socket, reactors and accept thread already exist. The rollback must
//      join, stop and release all of them, leaving the port free for a retry.
//   I. fail_reactor_start_after_for_test -- a reactor refusing after earlier
//      ones started must leave no partial set (epoll/eventfd/thread) behind,
//      and the retry must succeed.
//   J. reactor_stall_timeout_ms -- per-reactor stats sum to the lifetime
//      aggregate, survive a restart, zero their connection counts on teardown,
//      and expose a blocked reactor without stopping the server.
//   K. fail_worker_reactor_for_test -- quarantining the reactor that owns a
//      connection resets that peer with the close reason "reactor failure"
//      while the surviving reactor keeps the server running.
//
// Build (Linux only), one line, so nothing here can be copied wrong:
//   g++ -std=c++17 -Wall -Wextra -pthread verify/fault_injection.cpp -o fault_injection
//
// No -D any more. The switches used to live behind INDUSTRIAL_TCP_TESTING,
// which made the class definition depend on a build flag -- an ODR hazard, and
// the reason they became unconditional members. They are now private members
// reachable only through industrial::test_access (verify/harness.h), so there
// is nothing left for the macro to unlock and the file builds like every other
// suite.
//
// Exit code 0 = all cases passed.
#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::multi_reactor_server;

namespace {

using harness::free_port;

// Runs fn elsewhere and gives it `ms` to finish.
//
// A recovery that deadlocks otherwise presents as a test that takes a long time,
// and nothing in the exit code distinguishes that from a slow machine. On timeout
// this leaves the process immediately rather than returning: the future from
// std::async blocks in its destructor until the task finishes, so the only way to
// report a hang is to not wait for it.
template <class F>
bool within(int ms, const char* what, F&& fn) {
    std::atomic<bool> done{false};
    std::thread t([&] { fn(); done.store(true); });
    t.detach();
    for (int i = 0; i < ms / 10; ++i) {
        if (done.load()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::printf("  [FAIL] %s did not return within %d ms\n", what, ms);
    std::fflush(stdout);
    std::_Exit(1);
}

bool wait_for(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// Anything still accepting here? Answers "was the listening socket actually
// released" without depending on how a subsequent bind would fail.
bool port_is_listening(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    // One close, always. This used to close twice when the connect succeeded,
    // and the second close could land on a descriptor number the accept loop
    // had just handed out -- which TSan reported, correctly, as a race between
    // this close() and accept4() on the same slot.
    const bool ok = ::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) == 0;
    ::close(fd);
    return ok;
}

// Open descriptors in this process, counted through /proc. opendir() itself
// holds one, but every count includes exactly one, so deltas are comparable.
int count_open_fds() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) return -1;
    int n = 0;
    while (const struct dirent* e = ::readdir(dir)) {
        if (std::strcmp(e->d_name, ".") == 0 ||
            std::strcmp(e->d_name, "..") == 0)
            continue;
        ++n;
    }
    ::closedir(dir);
    return n;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    multi_reactor_server::config base;
    base.bind_addr = "127.0.0.1";
    base.num_reactors = 2;

    // ---- A: one reactor throwing out of its loop ----------------------------
    //
    // The failpoint is consumed by an exchange, so exactly one reactor throws
    // even when both are due to run. That makes "one quarantined, one serving"
    // an assertion about the design rather than about the scheduler.
    {
        std::printf("A. one reactor leaves its loop; the survivor serves\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        cfg.num_reactors = 2;
        cfg.tick_ms = 20;
        multi_reactor_server server(cfg);
        harness::expect(server.start(), "start() ok");
        const uint64_t before = server.stats().reactor_failures.load();
        harness::expect(server.is_running(), "running before the fault");

        auto dead_slots = [&](size_t* count, size_t* first) {
            *count = 0;
            *first = 0;
            const auto v = server.reactor_stats();
            for (size_t i = 0; i < v.size(); ++i) {
                if (!v[i].dead) continue;
                if (*count == 0) *first = i;
                ++*count;
            }
        };

        industrial::test_access::fail_worker_once(server);
        harness::expect(wait_for(3000, [&] {
                            size_t n = 0, first = 0;
                            dead_slots(&n, &first);
                            return n == 1 &&
                                   server.stats().reactor_failures.load() ==
                                       before + 1;
                        }),
                        "exactly one reactor was quarantined");
        harness::expect(server.is_running(),
                        "the server keeps running while a reactor survives");

        // The survivor is the only reactor pick_reactor() can hand the new
        // connection to, so a PING answered here proves it is serving.
        harness::peer p;
        harness::expect(static_cast<bool>(p.connect(server.port())),
                        "a client connects");
        harness::expect(static_cast<bool>(
                            p.send(industrial::frame_type::PING, nullptr, 0)),
                        "PING sent");
        harness::frame reply;
        harness::expect(static_cast<bool>(p.expect_frame(&reply, 2000)),
                        "the surviving reactor answered");
        harness::expect(reply.type == industrial::frame_type::PONG,
                        "the answer is a PONG");

        // A second failure leaves no reactor alive, which is fatal.
        industrial::test_access::fail_worker_once(server);
        harness::expect(wait_for(3000, [&] { return !server.is_running(); }),
                        "the last reactor's failure stops the server");
        harness::expect(server.stats().reactor_failures.load() == before + 2,
                        "the second failure was counted exactly once");
        {
            size_t n = 0, first = 0;
            dead_slots(&n, &first);
            harness::expect(n == 2, "both reactor slots report dead");
        }
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
                        "stop() still completes");

        harness::expect(server.start(),
                        "the server restarts after a quarantine");
        harness::expect(server.is_running(), "the restarted server runs");
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
                        "the second stop completes");
        p.close();
    }

    // ---- B: the accept loop throwing ----------------------------------------
    {
        std::printf("B. the accept loop leaves through an exception\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        const uint16_t port = cfg.port;
        multi_reactor_server server(cfg);
        harness::expect(server.start(), "start() ok");
        harness::expect(port_is_listening(port), "listening before the fault");
        const uint64_t before = server.stats().reactor_failures.load();

        industrial::test_access::fail_accept_once(server);
        harness::expect(wait_for(3000, [&] { return !server.is_running(); }),
               "the server stops reporting itself as running");
        harness::expect(server.stats().reactor_failures.load() > before,
               "reactor_failures counted it");
        harness::expect(wait_for(3000, [&] { return !port_is_listening(port); }),
               "the listening socket it owned was released");
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
               "stop() still completes");
    }

    // ---- C: fails, restarts, fails again ------------------------------------
    {
        std::printf("C. 12 rounds of start -> fault -> stop -> start\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        // One reactor, so there is no question about which worker picked it up
        // and no round passes by targeting a worker that has already left.
        cfg.num_reactors = 1;
        multi_reactor_server server(cfg);

        int usable_again = 0;
        for (int round = 0; round < 12; ++round) {
            if (!server.start()) {
                std::printf("  [FAIL] round %d: start() refused after %d "
                            "recoveries\n", round, usable_again);
                ++harness::failure_count();
                break;
            }
            const uint64_t before = server.stats().reactor_failures.load();
            if (round % 2 == 0) industrial::test_access::fail_worker_once(server);
            else                industrial::test_access::fail_accept_once(server);
            if (!wait_for(3000, [&] { return !server.is_running(); })) {
                std::printf("  [FAIL] round %d: the fault never took effect\n", round);
                ++harness::failure_count();
                break;
            }
            if (server.stats().reactor_failures.load() <= before) {
                std::printf("  [FAIL] round %d: reactor_failures did not move\n", round);
                ++harness::failure_count();
                break;
            }
            if (!within(3000, "stop()", [&] { server.stop(); })) break;
            ++usable_again;
        }
        harness::expect(usable_again == 12, "every round left a server that could start again");
        std::printf("      completed %d of 12 rounds\n", usable_again);
    }

    // ---- D: a configuration that cannot be honoured is refused --------------
    {
        std::printf("D. close_linger_ms <= 0 is refused at start()\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();

        cfg.close_linger_ms = 0;
        multi_reactor_server server0(cfg);
        harness::expect(!server0.start(), "close_linger_ms=0 refused");
        harness::expect(!server0.is_running(), "a refused start leaves the server stopped");

        cfg.close_linger_ms = -1;
        multi_reactor_server server_neg(cfg);
        harness::expect(!server_neg.start(), "close_linger_ms=-1 refused");

        cfg.close_linger_ms = 1;
        multi_reactor_server server_ok(cfg);
        harness::expect(server_ok.start(), "close_linger_ms=1 still accepted");
        if (server_ok.is_running())
            harness::expect(within(3000, "stop()", [&] { server_ok.stop(); }),
                            "stop() completes");

        // The one accept-side setting that silently degrades: Linux clamps
        // backlog only from above (somaxconn), so 0 goes through as a
        // zero-length queue.
        cfg.close_linger_ms = 5000;
        cfg.backlog = 0;
        multi_reactor_server server_b_0(cfg);
        harness::expect(!server_b_0.start(), "backlog=0 refused");

        cfg.backlog = -1;
        multi_reactor_server server_b_neg(cfg);
        harness::expect(!server_b_neg.start(), "backlog=-1 refused");

        cfg.backlog = 1;
        multi_reactor_server server_b_ok(cfg);
        harness::expect(server_b_ok.start(), "backlog=1 still accepted");
        if (server_b_ok.is_running())
            harness::expect(within(3000, "stop()", [&] { server_b_ok.stop(); }),
                            "stop() completes");
    }

    // ---- E: a message handler that throws --------------------------------
    {
        std::printf("E. a throwing message handler is the application's to keep\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        cfg.num_reactors = 1;
        std::atomic<int> handled{0};
        std::atomic<int> threw{0};

        harness::fixture f(
            cfg,
            [&](uint64_t, const uint8_t*, size_t) {
                const int n = handled.fetch_add(
                                  1, std::memory_order_relaxed) + 1;
                if (n == 1) {
                    threw.fetch_add(1, std::memory_order_relaxed);
                    throw std::runtime_error("application message handler");
                }
            });
        harness::expect(f.started(), "start() ok");
        const uint64_t failures_before = f.stats().reactor_failures.load();

        harness::peer p;
        harness::expect(static_cast<bool>(p.connect(f.port())), "connect()");
        harness::expect(static_cast<bool>(p.send(
                            industrial::frame_type::DATA, "one", 3)),
                        "first frame sent");
        harness::expect(wait_for(3000, [&] { return threw.load() == 1; }),
                        "the handler ran and threw");
        harness::expect(f.server().is_running(),
                        "the server still reports itself as running");
        harness::expect(f.stats().reactor_failures.load() == failures_before,
                        "reactor_failures did not move");

        harness::expect(static_cast<bool>(p.send(
                            industrial::frame_type::DATA, "two", 3)),
                        "second frame sent on the same connection");
        harness::expect(wait_for(3000, [&] { return handled.load() >= 2; }),
                        "the connection kept dispatching after the throw");
    }

    // ---- F: a refused close request is still honoured -----------------------
    //
    // close_connection() hands the request to the reactor queue. When the
    // queue refuses it, the old behaviour was a rollback and a counter -- the
    // connection stayed open until idle timeout. close_wanted now records the
    // request on the connection, and do_tick's sweep performs it within one
    // tick. The switch below makes submit_close() refuse once, which is the
    // only way to reach the path on an otherwise empty queue.
    {
        std::printf("F. a queue-refused close still reaches the connection\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        cfg.num_reactors = 1;
        cfg.tick_ms = 20;
        cfg.idle_timeout_ms = 60000;
        cfg.close_linger_ms = 1000;

        std::atomic<uint64_t> id{0};
        std::atomic<int> closed{0};
        harness::fixture f(
            cfg,
            [&](uint64_t cid, const uint8_t*, size_t) { id.store(cid); },
            [&](uint64_t, const std::string&, const char*) {
                closed.fetch_add(1, std::memory_order_relaxed);
            });
        harness::expect(f.started(), "start() ok");
        if (!f.started()) return harness::finish();

        harness::peer p;
        harness::expect(static_cast<bool>(p.connect(f.port())), "client connected");
        harness::expect(static_cast<bool>(p.send(
                            industrial::frame_type::DATA, "x", 1)),
                        "request sent");
        harness::expect(wait_for(2000, [&] { return id.load() != 0; }),
                        "connection id observed");
        if (id.load() == 0) return harness::finish();

        const uint64_t dropped_before =
            f.stats().close_requests_dropped.load();
        industrial::test_access::fail_close_enqueue_once(f.server());
        // The queue refused the hand-off, but the request is honoured by the
        // tick sweep, so close_connection() reports acceptance. Everything
        // below this line -- the counted refusal, the FIN, the gone connection
        // -- is what the acceptance promises.
        const bool accepted = f.server().close_connection(id.load());
        harness::expect(accepted,
                        "close_connection() accepts a request the queue refused");
        harness::expect(f.stats().close_requests_dropped.load() >
                            dropped_before,
                        "the refusal is counted");
        harness::expect(f.server().connection_ids().size() == 1,
                        "the refused request did not close inline");

        harness::expect(static_cast<bool>(
                            p.expect_close(harness::close_kind::fin, 2000)),
                        "the swept close still sent a FIN");
        p.close();
        harness::expect(wait_for(2000, [&] { return closed.load() >= 1; }),
                        "the close reached the callback");
        harness::expect(wait_for(2000, [&] {
                            return f.server().connection_count() == 0;
                        }),
                        "the connection is gone");
    }

    // ---- G: a failed accept setup releases the spare descriptor ------------
    //
    // spare_fd_ is opened before the listen and wake registrations. An early
    // return there used to skip accept_loop's close block, and start()'s
    // recovery does not know about the spare; after a failed start the
    // lifecycle is `stopped`, so stop() returns before cleaning up either.
    // Count descriptors across the failure, the retry and the destructor.
    {
        std::printf("G. a failed start releases the spare descriptor\n");
        const int before = count_open_fds();
        harness::expect(before > 0, "the process descriptor count is readable");
        if (before > 0) {
            {
                multi_reactor_server::config cfg = base;
                cfg.num_reactors = 1;
                multi_reactor_server server(cfg);
                industrial::test_access::fail_epoll_add_once(server);
                bool started = true;
                harness::expect(
                    within(3000, "start() (epoll add fails)",
                           [&] { started = server.start(); }),
                    "start() returns");
                harness::expect(!started,
                                "start() fails when epoll registration fails");
                const int after_fail = count_open_fds();
                harness::expect(after_fail == before,
                                "the failed start leaked no descriptors");

                bool retried = false;
                harness::expect(
                    within(3000, "start() (retry)",
                           [&] { retried = server.start(); }),
                    "the retry returns");
                harness::expect(retried, "the retry starts");
                harness::expect(server.is_running(), "the retried server runs");
                harness::expect(within(3000, "stop()",
                                       [&] { server.stop(); }),
                                "stop() completes");
                const int after_stop = count_open_fds();
                std::printf("      open fds: before=%d after_fail=%d "
                            "after_stop=%d\n", before, after_fail, after_stop);
                harness::expect(after_stop == before,
                                "the retried server released its descriptors");
            }
            harness::expect(count_open_fds() == before,
                            "destruction leaked no descriptors");
        }
    }

    // ---- H: a failed handler-pool start rolls a live server back -----------
    //
    // By the time start_handler_pool() refuses, the listening socket, every
    // reactor and the accept thread exist. The rollback must join the accept
    // thread, stop the reactors, release every descriptor and leave the port
    // bindable so the retry can succeed.
    {
        std::printf("H. a failed handler-pool start rolls back completely\n");
        const int before = count_open_fds();
        const uint16_t port = free_port();
        harness::expect(before > 0 && port != 0,
                        "descriptor count and port are available");
        if (before > 0 && port != 0) {
            {
                multi_reactor_server::config cfg = base;
                cfg.port = port;
                cfg.num_reactors = 1;
                cfg.handler_threads = 1;
                cfg.handler_queue_limit = 64;
                multi_reactor_server server(cfg);
                industrial::test_access::fail_handler_pool_once(server);
                bool started = true;
                harness::expect(
                    within(3000, "start() (handler pool refuses)",
                           [&] { started = server.start(); }),
                    "start() returns");
                harness::expect(!started,
                                "start() fails when the handler pool refuses");
                harness::expect(!port_is_listening(port),
                                "the failed start released the listen socket");
                const int after_fail = count_open_fds();
                harness::expect(after_fail == before,
                                "the failed start leaked no descriptors");

                bool retried = false;
                harness::expect(
                    within(3000, "start() (retry)",
                           [&] { retried = server.start(); }),
                    "the retry returns");
                harness::expect(retried, "the retry starts");
                harness::expect(server.is_running(), "the retried server runs");
                harness::expect(within(3000, "stop()",
                                       [&] { server.stop(); }),
                                "stop() completes");
                const int after_stop = count_open_fds();
                std::printf("      open fds: before=%d after_fail=%d "
                            "after_stop=%d\n", before, after_fail, after_stop);
                harness::expect(after_stop == before,
                                "the retried server released its descriptors");
            }
            harness::expect(count_open_fds() == before,
                            "destruction leaked no descriptors");
        }
    }

    // ---- I: a partially started reactor set is rolled back ------------------
    //
    // The failpoint refuses the second reactor, after the first one is
    // running. The rollback must stop the started subset, release its epoll,
    // eventfd and thread, and leave the port bindable for the retry.
    {
        std::printf("I. a partial reactor set is rolled back\n");
        const int before = count_open_fds();
        const uint16_t port = free_port();
        harness::expect(before > 0 && port != 0,
                        "descriptor count and port are available");
        if (before > 0 && port != 0) {
            {
                multi_reactor_server::config cfg = base;
                cfg.port = port;
                cfg.num_reactors = 2;
                multi_reactor_server server(cfg);
                industrial::test_access::fail_reactor_start_after(server, 1);
                bool started = true;
                harness::expect(
                    within(3000, "start() (second reactor refuses)",
                           [&] { started = server.start(); }),
                    "start() returns");
                harness::expect(!started,
                                "start() fails when the second reactor refuses");
                harness::expect(!port_is_listening(port),
                                "the failed start released the listen socket");
                const int after_fail = count_open_fds();
                harness::expect(after_fail == before,
                                "the partial reactor set leaked no descriptors");

                bool retried = false;
                harness::expect(
                    within(3000, "start() (retry)",
                           [&] { retried = server.start(); }),
                    "the retry returns");
                harness::expect(retried, "the retry starts");
                harness::expect(server.is_running(), "the retried server runs");
                harness::expect(within(3000, "stop()",
                                       [&] { server.stop(); }),
                                "stop() completes");
                const int after_stop = count_open_fds();
                std::printf("      open fds: before=%d after_fail=%d "
                            "after_stop=%d\n", before, after_fail, after_stop);
                harness::expect(after_stop == before,
                                "the retried server released its descriptors");
            }
            harness::expect(count_open_fds() == before,
                            "destruction leaked no descriptors");
        }
    }

    // ---- J: per-reactor stats and the stall watchdog ------------------------
    //
    // The slots must sum to the lifetime aggregate, survive a restart with the
    // counters intact, zero their connection counts on teardown, and expose a
    // blocked reactor without making it fatal. A timeout below
    // max(100, 2 * tick_ms) is refused: a healthy interval is one tick plus the
    // loop body. A later run that resolves fewer reactors must keep the
    // surplus slots visible, and a start that fails after allocation must keep
    // them reconcilable with the lifetime aggregate.
    {
        std::printf("J. per-reactor stats and the stall watchdog\n");

        {
            multi_reactor_server::config bad = base;
            bad.port = free_port();
            bad.tick_ms = 500;
            bad.reactor_stall_timeout_ms = 100;
            multi_reactor_server s(bad);
            harness::expect(!s.start(),
                            "a stall timeout below the floor is refused");
            harness::expect(s.reactor_stats().empty(),
                            "a refused start publishes no slots");

            bad.port = free_port();
            bad.tick_ms = 20;
            bad.reactor_stall_timeout_ms = 21;
            multi_reactor_server hair_trigger(bad);
            harness::expect(!hair_trigger.start(),
                            "tick_ms + 1 does not pass the watchdog floor");
            harness::expect(hair_trigger.reactor_stats().empty(),
                            "the second refusal publishes no slots either");
        }

        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        cfg.tick_ms = 20;
        cfg.reactor_stall_timeout_ms = 100;
        std::atomic<bool> in_handler{false};
        std::atomic<bool> release{false};
        multi_reactor_server server(cfg);
        // Only "x" blocks. The routing case below needs the other reactor to
        // keep serving while this one is stuck in here.
        server.set_message_handler([&](uint64_t, const uint8_t* p, size_t n) {
            if (n != 1 || p[0] != 'x') return;
            in_handler.store(true, std::memory_order_release);
            for (int i = 0; i < 3000 &&
                            !release.load(std::memory_order_acquire); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
        harness::expect(server.start(), "start() ok");
        harness::expect(server.reactor_stats().size() == 2,
                        "one stats slot per reactor");

        harness::peer peer;
        harness::expect(static_cast<bool>(peer.connect(server.port())),
                        "client connected");
        harness::expect(static_cast<bool>(
                            peer.send(industrial::frame_type::DATA, "x", 1)),
                        "frame sent");
        harness::expect(wait_for(2000, [&] {
                            return in_handler.load(std::memory_order_acquire);
                        }), "the handler is blocking a reactor");
        harness::expect(wait_for(2500, [&] {
                            return server.stats().reactor_stalls.load() >= 1;
                        }), "a stalled reactor was counted");
        harness::expect(server.is_running(),
                        "a stalled reactor does not stop the server");

        // Routing. Six connections opened while one reactor is blocked must all
        // land on the other one. Without the skip the round-robin hands every
        // second connection to the blocked reactor, where it waits in a queue
        // nobody drains until this handler returns -- a peer watching a live
        // TCP connection that never answers.
        {
            std::vector<std::unique_ptr<harness::peer>> routed;
            for (int i = 0; i < 6; ++i) {
                auto p = std::make_unique<harness::peer>();
                if (!p->connect(server.port())) break;
                (void)p->send(industrial::frame_type::DATA, "y", 1);
                routed.push_back(std::move(p));
            }
            harness::expect(routed.size() == 6, "six connections opened");
            size_t stalled_conns = 0, live_conns = 0;
            bool   saw_stalled = false;
            harness::expect(wait_for(2000, [&] {
                const auto v = server.reactor_stats();
                stalled_conns = live_conns = 0;
                saw_stalled = false;
                for (const auto& r : v) {
                    if (r.retired) continue;
                    if (r.stalled) {
                        saw_stalled = true;
                        stalled_conns += r.connections;
                    } else {
                        live_conns += r.connections;
                    }
                }
                return saw_stalled && stalled_conns == 1 && live_conns == 6;
            }), "new connections are routed to the live reactor");
            harness::expect(server.stats().reactor_stall_skips.load() >= 1,
                            "the reroutes are counted");
        }

        release.store(true, std::memory_order_release);
        // Hysteresis. The flag is not cleared by the first healthy sample: the
        // reactor has to look fresh by half a window, and a full window has to
        // have passed since the stall was declared. One episode stays one
        // episode -- a re-admit-and-drop cycle would show up as reactor_stalls
        // climbing past 1.
        harness::expect(wait_for(3000, [&] {
                            for (const auto& r : server.reactor_stats())
                                if (!r.retired && r.stalled) return false;
                            return true;
                        }), "the stalled reactor is re-admitted after recovery");
        harness::expect(server.stats().reactor_stalls.load() == 1,
                        "one episode, counted once");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        {
            const auto v = server.reactor_stats();
            uint64_t recv = 0, sent = 0, brecv = 0, bsent = 0;
            size_t   conns = 0;
            bool     progress = false, dead = false;
            for (const auto& r : v) {
                recv += r.frames_recv;
                sent += r.frames_sent;
                brecv += r.bytes_recv;
                bsent += r.bytes_sent;
                conns += r.connections;
                if (r.last_progress_ms != 0) progress = true;
                dead = dead || r.dead;
            }
            const auto& st = server.stats();
            harness::expect(recv == st.frames_recv.load(),
                            "per-reactor frames_recv sums to the aggregate");
            harness::expect(sent == st.frames_sent.load(),
                            "per-reactor frames_sent sums to the aggregate");
            harness::expect(brecv == st.bytes_recv.load(),
                            "per-reactor bytes_recv sums to the aggregate");
            harness::expect(bsent == st.bytes_sent.load(),
                            "per-reactor bytes_sent sums to the aggregate");
            harness::expect(conns == server.connection_count(),
                            "per-reactor connections sum to the count");
            harness::expect(progress,
                            "reactors publish a progress timestamp");
            harness::expect(!dead, "a healthy reactor is not marked dead");
        }

        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
                        "stop() completes");
        {
            const auto v = server.reactor_stats();
            uint64_t recv = 0;
            size_t   conns = 0;
            for (const auto& r : v) {
                recv += r.frames_recv;
                conns += r.connections;
            }
            harness::expect(conns == 0 && server.connection_count() == 0,
                            "connections are zero after stop");
            harness::expect(recv == server.stats().frames_recv.load(),
                            "traffic counters remain cumulative across stop");
        }

        harness::expect(server.start(), "the restart starts");
        {
            const auto v = server.reactor_stats();
            uint64_t recv = 0;
            for (const auto& r : v) recv += r.frames_recv;
            harness::expect(v.size() == 2, "the restart republishes both slots");
            harness::expect(recv == server.stats().frames_recv.load(),
                            "traffic counters stay cumulative across restart");
        }
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
                        "the second stop completes");
        peer.close();

        // A quarantine can land between the watchdog's dead check and its
        // publish. The stronger state must win: the watchdog must withdraw the
        // stall flag and must not count a reactor that died before the flag
        // was published.
        {
            multi_reactor_server::config cfg_race = base;
            cfg_race.port = free_port();
            cfg_race.tick_ms = 20;
            cfg_race.reactor_stall_timeout_ms = 100;
            cfg_race.num_reactors = 2;
            std::atomic<bool> race_handler{false};
            std::atomic<bool> race_release{false};
            multi_reactor_server race(cfg_race);
            race.set_message_handler(
                [&](uint64_t, const uint8_t*, size_t) {
                    race_handler.store(true, std::memory_order_release);
                    for (int i = 0; i < 3000 &&
                                    !race_release.load(
                                        std::memory_order_acquire); ++i) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(1));
                    }
                });
            harness::expect(race.start(),
                            "the watchdog-race server starts");

            harness::peer rp;
            harness::expect(static_cast<bool>(rp.connect(race.port())),
                            "the watchdog-race client connected");
            int owner = -1;
            harness::expect(wait_for(2000, [&] {
                const auto v = race.reactor_stats();
                for (size_t i = 0; i < v.size(); ++i) {
                    if (!v[i].retired && v[i].connections == 1) {
                        owner = static_cast<int>(i);
                        return true;
                    }
                }
                return false;
            }), "the race connection has an owner");

            if (owner >= 0) {
                industrial::test_access::pause_stall_check(race, owner);
                harness::expect(static_cast<bool>(rp.send(
                                    industrial::frame_type::DATA, "x", 1)),
                                "the blocking frame is sent");
                harness::expect(wait_for(2000, [&] {
                                    return race_handler.load(
                                        std::memory_order_acquire);
                                }), "the handler is blocking the owner");
                harness::expect(wait_for(3000, [&] {
                                    return industrial::test_access::
                                        stall_check_entered(race);
                                }), "the watchdog is parked before publishing");

                const uint64_t stalls_before =
                    race.stats().reactor_stalls.load();
                industrial::test_access::fail_reactor_now(race, owner);
                harness::expect(wait_for(2000, [&] {
                    const auto v = race.reactor_stats();
                    return static_cast<size_t>(owner) < v.size() &&
                           v[owner].dead && !v[owner].stalled;
                }), "the interleaved quarantine wins over the stall");

                const uint64_t finished =
                    industrial::test_access::stall_check_finished(race);
                industrial::test_access::pause_stall_check(race, -1);
                harness::expect(wait_for(2000, [&] {
                    return industrial::test_access::stall_check_finished(race) >
                           finished;
                }), "the parked watchdog call returns");
                {
                    const auto v = race.reactor_stats();
                    harness::expect(
                        static_cast<size_t>(owner) < v.size() &&
                            v[owner].dead && !v[owner].stalled,
                        "a dead slot is not left flagged stalled");
                    harness::expect(
                        race.stats().reactor_stalls.load() == stalls_before,
                        "the interleaved quarantine is not a stall");
                }
            }

            race_release.store(true, std::memory_order_release);
            rp.close();
            harness::expect(within(3000, "stop()", [&] { race.stop(); }),
                            "the watchdog-race server stops");
        }

        // A later run that resolves fewer reactors must not hide the traffic
        // already published into the surplus slots: their counters are
        // cumulative, and the per-reactor sums must keep matching
        // server_stats().
        {
            multi_reactor_server::config cfg_small = base;
            cfg_small.port = free_port();
            cfg_small.tick_ms = 20;
            cfg_small.reactor_stall_timeout_ms = 100;
            cfg_small.num_reactors = 4;
            multi_reactor_server shrinking(cfg_small);
            shrinking.set_message_handler(
                [](uint64_t, const uint8_t*, size_t) {});
            harness::expect(shrinking.start(), "the four-reactor run starts");
            harness::expect(shrinking.reactor_stats().size() == 4,
                            "four slots are published");

            // One connection per reactor's round-robin turn, so every slot
            // receives a frame before the resolved count shrinks.
            std::vector<harness::peer> peers;
            peers.reserve(4);
            const uint64_t recv_before = shrinking.stats().frames_recv.load();
            for (int i = 0; i < 4; ++i) {
                peers.emplace_back();
                harness::expect(
                    static_cast<bool>(
                        peers.back().connect(shrinking.port())),
                    "a client connected");
                harness::expect(static_cast<bool>(peers.back().send(
                                    industrial::frame_type::DATA, "x", 1)),
                                "frame sent");
            }
            harness::expect(wait_for(2000, [&] {
                                return shrinking.stats().frames_recv.load() >=
                                       recv_before + 4;
                            }),
                            "all four reactors received a frame");

            harness::expect(within(3000, "stop()",
                                   [&] { shrinking.stop(); }),
                            "the four-reactor run stops");

            industrial::test_access::force_reactor_count(shrinking, 2);
            harness::expect(shrinking.start(), "the shrunken run starts");
            {
                const auto v = shrinking.reactor_stats();
                harness::expect(v.size() == 4,
                                "the surplus slots stay published");
                if (v.size() == 4) {
                    harness::expect(!v[0].retired && !v[1].retired,
                                    "the two current slots are active");
                    harness::expect(v[2].retired && v[3].retired,
                                    "the surplus slots are retired");
                    harness::expect(v[2].connections == 0 &&
                                        v[3].connections == 0,
                                    "retired slots report no connections");
                    harness::expect(v[2].last_progress_ms == 0 &&
                                        v[3].last_progress_ms == 0,
                                    "retired slots report no progress stamp");
                    harness::expect(v[2].frames_recv > 0 &&
                                        v[3].frames_recv > 0,
                                    "the retired slots keep their traffic");
                }
                uint64_t recv = 0, sent = 0, brecv = 0, bsent = 0;
                size_t conns = 0;
                for (const auto& r : v) {
                    recv += r.frames_recv;
                    sent += r.frames_sent;
                    brecv += r.bytes_recv;
                    bsent += r.bytes_sent;
                    conns += r.connections;
                }
                const auto& st = shrinking.stats();
                harness::expect(recv == st.frames_recv.load(),
                                "the shrunken sums still match frames_recv");
                harness::expect(sent == st.frames_sent.load(),
                                "the shrunken sums still match frames_sent");
                harness::expect(brecv == st.bytes_recv.load(),
                                "the shrunken sums still match bytes_recv");
                harness::expect(bsent == st.bytes_sent.load(),
                                "the shrunken sums still match bytes_sent");
                harness::expect(conns == shrinking.connection_count(),
                                "the shrunken sums still match connections");
            }
            harness::expect(within(3000, "stop()",
                                   [&] { shrinking.stop(); }),
                            "the shrunken run stops");

            // A start that fails after the slots exist must leave them
            // published and retired, not empty: the lifetime counters are
            // still in the aggregate and must remain reconcilable.
            industrial::test_access::fail_handler_pool_once(shrinking);
            bool failed = true;
            harness::expect(
                within(3000, "start() (handler pool refuses)",
                       [&] { failed = shrinking.start(); }),
                "the refused start returns");
            harness::expect(!failed, "the start fails after allocation");
            {
                const auto v = shrinking.reactor_stats();
                harness::expect(v.size() == 4,
                                "a failed start keeps the slots published");
                bool retired = v.size() == 4;
                uint64_t recv = 0;
                for (const auto& r : v) {
                    retired = retired && r.retired;
                    recv += r.frames_recv;
                }
                harness::expect(retired,
                                "a failed start retires every slot");
                harness::expect(recv == shrinking.stats().frames_recv.load(),
                                "the failed start kept the sums reconcilable");
            }

            industrial::test_access::force_reactor_count(shrinking, -1);
            harness::expect(shrinking.start(),
                            "the server starts again after the refusal");
            harness::expect(within(3000, "stop()",
                                   [&] { shrinking.stop(); }),
                            "the final stop completes");
        }
    }

    // ---- K: quarantining a reactor resets its connections -------------------
    //
    // The connection is placed first and its owning reactor is read from
    // reactor_stats()[i].connections; the indexed failpoint then targets that
    // reactor. The peer must see a reset (the reactor is gone, so a graceful
    // FIN exchange cannot be completed) and the close callback must name
    // "reactor failure". The other reactor keeps the server running.
    {
        std::printf("K. a quarantined reactor resets its connections\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        cfg.num_reactors = 2;
        cfg.tick_ms = 20;
        cfg.idle_timeout_ms = 60000;
        cfg.close_linger_ms = 1000;
        // The reactor has to be *stalled* before it is quarantined, or the
        // assertion that quarantine drops the flag tests nothing.
        cfg.reactor_stall_timeout_ms = 100;

        std::atomic<uint64_t> id{0};
        std::atomic<int> closed{0};
        std::atomic<int> reason_ok{0};
        std::atomic<bool> in_handler{false};
        std::atomic<bool> release{false};
        multi_reactor_server server(cfg);
        server.set_message_handler(
            [&](uint64_t cid, const uint8_t* p, size_t n) {
                id.store(cid, std::memory_order_release);
                if (n == 1 && p[0] == 'x') {
                    in_handler.store(true, std::memory_order_release);
                    while (!release.load(std::memory_order_acquire))
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(1));
                }
            });
        server.set_close_handler(
            [&](uint64_t, const std::string&, const char* reason) {
                if (reason != nullptr &&
                    std::strcmp(reason, "reactor failure") == 0) {
                    reason_ok.fetch_add(1, std::memory_order_relaxed);
                }
                closed.fetch_add(1, std::memory_order_relaxed);
            });
        harness::expect(server.start(), "start() ok");

        harness::peer p;
        harness::expect(static_cast<bool>(p.connect(server.port())),
                        "client connected");
        harness::expect(static_cast<bool>(
                            p.send(industrial::frame_type::DATA, "x", 1)),
                        "frame sent");
        harness::expect(wait_for(2000, [&] { return id.load() != 0; }),
                        "the connection is admitted");
        harness::expect(wait_for(2000, [&] {
                            return in_handler.load(std::memory_order_acquire);
                        }), "the handler is blocking its reactor");

        // reactor_stats() is the only public way to learn which slot owns it.
        int owner = -1;
        harness::expect(wait_for(2000, [&] {
                            const auto v = server.reactor_stats();
                            for (size_t i = 0; i < v.size(); ++i) {
                                if (v[i].connections == 1) {
                                    owner = static_cast<int>(i);
                                    return true;
                                }
                            }
                            return false;
                        }),
                        "the owning reactor is identified");

        if (owner >= 0) {
            harness::expect(wait_for(2500, [&] {
                                const auto v = server.reactor_stats();
                                return static_cast<size_t>(owner) < v.size() &&
                                       v[owner].stalled;
                            }), "the watchdog marks the doomed reactor stalled");
            const uint64_t failures_before =
                server.stats().reactor_failures.load();
            // Arm the failure while the reactor is still inside the handler,
            // then let it return: its next loop iteration throws, so the slot
            // goes from stalled to dead with both flags set for an instant.
            industrial::test_access::fail_worker_reactor_once(server, owner);
            release.store(true, std::memory_order_release);
            harness::expect(wait_for(3000, [&] {
                                return server.stats().reactor_failures.load() >
                                       failures_before;
                            }),
                            "the targeted reactor failed");
            harness::expect(static_cast<bool>(
                                p.expect_close(harness::close_kind::rst, 2000)),
                            "its peer saw a reset, not a FIN");
            harness::expect(wait_for(2000, [&] {
                                return reason_ok.load() == 1 &&
                                       closed.load() == 1;
                            }),
                            "the close callback reported \"reactor failure\"");
            harness::expect(server.is_running(),
                            "the surviving reactor keeps the server running");
            {
                const auto v = server.reactor_stats();
                harness::expect(v.size() == 2 &&
                                    static_cast<size_t>(owner) < v.size() &&
                                    v[owner].dead,
                                "the targeted slot is marked dead");
                harness::expect(static_cast<size_t>(owner) < v.size() &&
                                    !v[owner].stalled,
                                "quarantine drops the stall flag with it");
            }
        }
        // If the owner was never identified the handler must still be let go,
        // or stop() joins a reactor parked in it and the suite hangs.
        release.store(true, std::memory_order_release);
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
                        "stop() completes");
        p.close();
    }

    return harness::finish();
}
