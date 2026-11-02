// verify/fault_injection.cpp -- what a worker dying is supposed to look like.
//
// INDUSTRIAL_TCP_TESTING adds two switches so these paths can be exercised, and
// until now nothing exercised them: two switches with no consumer, in a
// directory whose only other check was compile-only. A switch nobody flips is no
// more a test than a commented-out assert.
//
// Both switches raise exactly the same question, which is what recovery means
// for a thread that left its loop through an exception rather than a request:
//
//   A. fail_worker_for_test -- a reactor throwing out of loop() must be visible
//      (reactor_failures, is_running() false), must not take the process with it,
//      and must leave a stoppable server behind.
//   B. fail_accept_for_test -- same for accept_loop(), plus: the listening socket it
//      owned must go away, or the port is held by a server that is not listening.
//   C. Injecting across repeated restarts -- anything that claims a failed worker
//      is recoverable has to survive being restarted after one died, because that
//      is the only shape "recoverable" can take that anyone would rely on.
//   D. close_linger_ms <= 0 must be refused at start(): do_tick only sweeps a parked
//      close when linger > 0, so a non-positive value turns every peer that never
//      answers our FIN into a permanent fd + admission leak. Same for backlog <= 0:
//      listen() takes the value as given, and a zero-length accept queue looks
//      exactly like a server that randomly refuses connections under load.
//
// Build (Linux only), one line, so nothing here can be copied wrong:
//   g++ -std=c++17 -Wall -Wextra -pthread -DINDUSTRIAL_TCP_TESTING verify/fault_injection.cpp -o fault_injection
//
// The define is what unlocks the switches: without it this file does not compile,
// which is the intended failure mode -- a test that quietly stops testing is
// worse than one that refuses to build.
//
// Exit code 0 = all cases passed.
#include <arpa/inet.h>
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
#include <string>
#include <thread>
#include <vector>

#ifndef INDUSTRIAL_TCP_TESTING
#error "fault_injection.cpp requires -DINDUSTRIAL_TCP_TESTING; see the file header"
#endif

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

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    multi_reactor_server::config base;
    base.bind_addr = "127.0.0.1";
    base.num_reactors = 2;

    // ---- A: a reactor throwing out of its loop ------------------------------
    {
        std::printf("A. a reactor leaves its loop through an exception\n");
        multi_reactor_server::config cfg = base;
        cfg.port = free_port();
        multi_reactor_server server(cfg);
        harness::expect(server.start(), "start() ok");
        const uint64_t before = server.stats().reactor_failures.load();

        server.fail_worker_for_test.store(true);
        harness::expect(wait_for(3000, [&] { return !server.is_running(); }),
               "the server stops reporting itself as running");
        harness::expect(server.stats().reactor_failures.load() > before,
               "reactor_failures counted it");
        harness::expect(within(3000, "stop()", [&] { server.stop(); }),
               "stop() still completes");
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

        server.fail_accept_for_test.store(true);
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
            if (round % 2 == 0) server.fail_worker_for_test.store(true);
            else                server.fail_accept_for_test.store(true);
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
        if (server_ok.is_running()) server_ok.stop();

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
        if (server_b_ok.is_running()) server_b_ok.stop();
    }

    return harness::finish();
}
