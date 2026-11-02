// verify/send_results.cpp -- send_to()'s five answers, two counters, and the
// optional handler offload pool, asserted.
//
// send_to() is the whole of how an application puts bytes on a connection, and
// send_result is the whole of what it learns back. Four of the five values had no
// assertion anywhere under verify/: no case ever asked for not_connected,
// too_large, overloaded or invalid_argument. Correct and unguarded is a worse
// state than wrong -- any edit can change any of them and every configuration
// still comes back green, which is exactly how the round-6 review found them.
//
// Two counters were in the same position, and are covered here for the same
// reason:
//
//   Stats::accept_errors -- the accept loop's EMFILE/ENFILE path, the one accept
//   failure that can be predicted at startup, and the one that leaves no other
//   symptom than "it stopped accepting and said nothing".
//
//   The reactor's EPOLLHUP|EPOLLERR branch -- the only place a connection is
//   closed because the kernel says the socket is already broken, rather than
//   because either side asked.
//
// What is asserted:
//   1. Each of the five send_result values, on the state that produces it.
//      overloaded in particular is made a property of the frame (a ceiling of
//      32 bytes) rather than of timing, so it does not have to win a race
//      against the drain to be reached.
//   2. closed while a close is in progress: send_to() must not promise a frame
//      the owner has already decided to refuse (R6-3), and must not leave a
//      queue_drops behind while saying ok.
//   3. accept_errors climbs when the descriptor ceiling is lower than the number
//      of peers knocking, instead of the loop going quiet.
//   4. hup/err, asked rather than assumed: whether this host reports an aborted
//      connection at all is checked first, because an abort that arrives as an
//      ordinary FIN never reaches the branch, and asserting against a break the
//      host cannot produce would be asserting against the host.
//   5. The handler offload pool: offload() is refused when the pool is off or
//      its queue is full, tasks for one connection stay ordered even when a
//      later task is faster, a PING is answered while an offloaded task
//      sleeps -- the reactor is not the one doing the sleeping -- and a
//      throwing task is swallowed without taking its worker with it.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/send_results.cpp -o send_results
//
// Exit code 0 = every answer is the documented one. A [SKIP] is not a pass: it
// names what was asked and what the machine answered.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;
using industrial::send_result;

namespace {

// A case that asked the machine a question first and did not like the answer.
// It says which question. It is not a pass.
void skip(const char* what) {
    std::printf("  [SKIP] %s\n", what);
}

const char* name(send_result r) {
    switch (r) {
        case send_result::ok:              return "ok";
        case send_result::not_connected:    return "not_connected";
        case send_result::closed:          return "closed";
        case send_result::too_large:        return "too_large";
        case send_result::overloaded:      return "overloaded";
        case send_result::invalid_argument: return "invalid_argument";
    }
    return "?";
}

uint16_t pick_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (::bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) { ::close(fd); return 0; }
    socklen_t sl = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &sl);
    ::close(fd);
    return ntohs(sa.sin_port);
}

multi_reactor_server::config base_config() {
    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.num_reactors   = 1;
    cfg.tick_ms        = 50;
    cfg.idle_timeout_ms = 60000;   // nothing here may end by a timer
    cfg.close_linger_ms = 3000;
    return cfg;
}

// ---- 1. the five answers -------------------------------------------------
void case_five_outcomes() {
    std::printf("send_to(): one of each of the five answers\n");
    multi_reactor_server::config cfg = base_config();
    // A ceiling low enough that "over it" is a property of the frame rather
    // than of timing: any frame larger than 32 bytes is refused on sight, so
    // overloaded does not have to outrun the drain to be observed.
    cfg.max_pending_bytes = industrial::FRAME_HEADER_SIZE * 4;
    cfg.wbuf_capacity    = industrial::FRAME_HEADER_SIZE * 64;
    harness::fixture server(cfg, [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) { harness::expect(false, "client connected"); return; }
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    const std::vector<uint64_t> ids = server.server().connection_ids();
    if (ids.empty()) { harness::expect(false, "connection registered"); return; }
    const uint64_t id = ids[0];

    const std::string small(4, 'a');
    const std::string big(100, 'b');
    // Never issued, and never will be: ids are handed out from 1 upwards and
    // this case is the only thing running.
    const uint64_t never_issued = 999999;

    const send_result ok      = server.server().send_to(id, small.data(), small.size());
    const send_result unknown = server.server().send_to(never_issued, small.data(), small.size());
    const send_result too_big  = server.server().send_to(id, small.data(),
                                             industrial::MAX_FRAME_PAYLOAD + 1);
    const send_result no_data  = server.server().send_to(id, nullptr, 5);
    const send_result over    = server.server().send_to(id, big.data(), big.size());
    const bool offload_off =
        server.server().offload(id, [] {});
    harness::expect(!offload_off,
                    "offload with handler_threads == 0: refused, not queued");

    std::printf("      ok=%s not_connected=%s too_large=%s invalid_argument=%s "
                "overloaded=%s\n",
                name(ok), name(unknown), name(too_big), name(no_data), name(over));

    harness::expect(ok == send_result::ok, "a live connection: ok");
    harness::expect(unknown == send_result::not_connected, "an id that was never issued: not_connected");
    harness::expect(too_big == send_result::too_large, "a payload past MAX_FRAME_PAYLOAD: too_large");
    harness::expect(no_data == send_result::invalid_argument, "data == nullptr with len > 0: invalid_argument");
    harness::expect(over == send_result::overloaded, "a frame past max_pending_bytes: overloaded");

    // The fifth value, on the state R6-3 is about: a close this caller asked
    // for. Baselines are taken after the close is in effect, so the backlog the
    // close itself refuses is not confused with the send under test.
    server.server().close_connection(id);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    const unsigned long long qd0 = server.stats().queue_drops.load();
    const unsigned long long bd0 = server.stats().bytes_dropped.load();
    harness::expect(server.server().connection_count() == 1,
                    "the closing connection is still in the table");
    const send_result closing = server.server().send_to(id, small.data(), small.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::printf("      while closing: %s (queue_drops +%llu, bytes_dropped +%llu)\n",
                name(closing), server.stats().queue_drops.load() - qd0,
                server.stats().bytes_dropped.load() - bd0);
    harness::expect(closing == send_result::closed, "a connection whose close is in progress: closed");
    harness::expect(server.stats().queue_drops.load() - qd0 == 0 &&
           server.stats().bytes_dropped.load() - bd0 == 0,
           "and it bought no drops: the refusal happens before any accounting");

    peer.close();
    server.stop();
}

// ---- 2. accept_errors ----------------------------------------------------
// The ceiling is per-process, so the server has to be the one that runs out
// while something else keeps knocking: a forked child with a lowered hard
// limit, and this process opening peers it will never close.
int server_under_ceiling(uint16_t port) {
    // Both limits, not just the soft one: start() raises the soft limit toward
    // the hard one, and a ceiling it can lift is no ceiling at all.
    struct rlimit rl{};
    rl.rlim_cur = 20;
    rl.rlim_max = 20;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        std::printf("      child: setrlimit failed: %s\n", std::strerror(errno));
        std::fflush(stdout);
        return 2;
    }
    multi_reactor_server::config cfg;
    cfg.bind_addr        = "127.0.0.1";
    cfg.port            = port;
    cfg.num_reactors     = 1;
    cfg.max_connections  = 10000;   // the ceiling under test is descriptors, not admission
    cfg.tick_ms          = 50;
    cfg.idle_timeout_ms   = 60000;
    multi_reactor_server server(cfg);
    server.set_message_handler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) {
        std::printf("      child: start() failed\n");
        std::fflush(stdout);
        return 2;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    const unsigned long long acc  = server.stats().accepted.load();
    const unsigned long long errs = server.stats().accept_errors.load();
    std::printf("      child: accepted %llu, accept_errors %llu\n", acc, errs);
    std::fflush(stdout);
    server.stop();
    return errs > 0 ? 0 : 1;
}

void case_accept_errors() {
    std::printf("accept_errors: the accept loop under a descriptor ceiling\n");
    const uint16_t port = pick_port();
    if (port == 0) { harness::expect(false, "a port to bind"); return; }

    pid_t pid = ::fork();
    if (pid == 0) ::_exit(server_under_ceiling(port));

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    std::vector<harness::peer> peers;
    for (int i = 0; i < 40; ++i) {
        harness::peer p;
        if (p.connect(port)) peers.push_back(std::move(p));
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    const size_t connected = peers.size();   // read before the peers go away
    peers.clear();
    std::printf("      %zu peers connected before the child ran out\n", connected);

    harness::expect(WIFEXITED(status) && WEXITSTATUS(status) == 0,
           "refused accepts are counted, not swallowed");
}

// ---- 3. hup/err ---------------------------------------------------------
// Asked, not assumed. An abort that the host reports as an ordinary FIN never
// reaches the branch, so the question "can this host break a socket in a way
// epoll reports?" is settled with a raw socket pair before the server is
// involved at all.
bool kernel_reports_broken_socket() {
    const int ln = ::socket(AF_INET, SOCK_STREAM, 0);
    if (ln < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(ln, (struct sockaddr*)&sa, sizeof(sa)) < 0) { ::close(ln); return false; }
    ::listen(ln, 4);
    socklen_t sl = sizeof(sa);
    ::getsockname(ln, (struct sockaddr*)&sa, &sl);

    const int c = ::socket(AF_INET, SOCK_STREAM, 0);
    if (c < 0 || ::connect(c, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        if (c >= 0) ::close(c);
        ::close(ln);
        return false;
    }
    const int sv = ::accept(ln, nullptr, nullptr);
    if (sv < 0) { ::close(c); ::close(ln); return false; }

    const int ep = ::epoll_create1(0);
    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = sv;
    ::epoll_ctl(ep, EPOLL_CTL_ADD, sv, &ev);

    // Bytes the peer will not read, then an abort: an abort with unread data is
    // the shape the kernel answers with RST rather than FIN.
    const char pending[8] = "pending";
    ::send(sv, pending, sizeof(pending), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    struct linger lg{};
    lg.l_onoff  = 1;
    lg.l_linger = 0;
    ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    ::close(c);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    struct epoll_event out[4]{};
    const int n = ::epoll_wait(ep, out, 4, 300);
    bool broken = false;
    for (int i = 0; i < n; ++i) {
        if (out[i].events & (EPOLLHUP | EPOLLERR)) broken = true;
    }
    ::close(ep); ::close(sv); ::close(ln);
    return broken;
}

void case_hup_err() {
    std::printf("hup/err: a socket the kernel reports as already broken\n");
    if (!kernel_reports_broken_socket()) {
        skip("this host turns an aborted connection into an ordinary FIN as far "
             "as epoll is concerned, so EPOLLHUP|EPOLLERR is never delivered "
             "and this branch cannot be reached from here");
        return;
    }

    std::atomic<int> hups{0};
    harness::fixture server(
        base_config(),
        [](uint64_t, const uint8_t*, size_t) {},
        [&hups](uint64_t, const std::string&, const char* reason) {
            if (reason && std::strcmp(reason, "hup/err") == 0) hups.fetch_add(1);
        });
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) { harness::expect(false, "client connected"); return; }
    const int fd = peer.fd();
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    const std::vector<uint64_t> ids = server.server().connection_ids();
    if (ids.empty()) { harness::expect(false, "connection registered"); return; }

    // A request first, so the connection is one that has carried traffic in
    // both directions; then bytes the peer will not read; then the abort.
    if (!peer.send(frame_type::DATA, "hi", 2)) {
        harness::expect(false, "request sent");
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const std::string payload(64, 'x');
    (void)server.server().send_to(ids[0], payload.data(), payload.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    struct linger lg{};
    lg.l_onoff  = 1;
    lg.l_linger = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    peer.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    std::printf("      close reasons matching hup/err: %d\n", hups.load());
    harness::expect(hups.load() >= 1, "a broken socket is closed as hup/err, without a drain attempt");
    server.stop();
}

// Offloading a handler must move the slow work off the reactor without
// reordering a connection's frames. A's first task sleeps longer than its
// second, so concurrent execution for one connection would show up as replies
// out of order. B's PING, handled by the reactor, must still be answered while
// the first task runs.
void case_offload() {
    std::printf("handler offload: serial per connection, off the reactor\n");
    multi_reactor_server::config bad = base_config();
    bad.handler_threads   = 1;
    bad.handler_queue_limit = 0;
    multi_reactor_server refused(bad);
    harness::expect(!refused.start(),
                    "handler_threads without a queue limit: start() refuses");

    multi_reactor_server::config cfg = base_config();
    cfg.handler_threads   = 2;
    cfg.handler_queue_limit = 32;

    std::atomic<multi_reactor_server*> srv{nullptr};
    std::atomic<int> offloaded{0};
    harness::fixture server(
        cfg, [&](uint64_t id, const uint8_t* p, size_t n) {
            std::vector<uint8_t> payload(p, p + n);
            multi_reactor_server* s = srv.load();
            if (!s) return;
            const bool ok = s->offload(id, [s, id, payload] {
                // The first request is deliberately the slower one.
                const int delay_ms =
                    (!payload.empty() && payload[0] == '1') ? 300 : 100;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(delay_ms));
                (void)s->send_to(id, payload.data(), payload.size());
            });
            if (ok) offloaded.fetch_add(1);
        });
    if (!server.started()) { harness::expect(false, "start() ok"); return; }
    srv.store(&server.server());

    harness::peer a, b;
    if (!a.connect(server.port()) || !b.connect(server.port())) {
        harness::expect(false, "clients connected");
        return;
    }

    const std::string first(4, '1');
    const std::string second(4, '2');
    harness::expect(static_cast<bool>(
                        a.send(frame_type::DATA, first.data(), first.size())),
                    "first request sent");
    harness::expect(static_cast<bool>(
                        a.send(frame_type::DATA, second.data(), second.size())),
                    "second request sent");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto t0 = std::chrono::steady_clock::now();
    harness::expect(static_cast<bool>(b.send(frame_type::PING, nullptr, 0)),
                    "B sends PING");
    harness::frame pong;
    const harness::status ps = b.expect_frame(&pong, 800);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    harness::expect(static_cast<bool>(ps), "B got a PONG", ps.why);
    harness::expect(pong.type == frame_type::PONG, "the answer is a PONG");
    harness::expect(elapsed < 250,
                    "the reactor answered while the offload task slept");

    harness::frame f1, f2;
    const harness::status s1 = a.expect_frame(&f1, 3000);
    const harness::status s2 = a.expect_frame(&f2, 3000);
    harness::expect(static_cast<bool>(s1), "first reply arrived", s1.why);
    harness::expect(static_cast<bool>(s2), "second reply arrived", s2.why);
    harness::expect(std::string(f1.payload.begin(), f1.payload.end()) == first,
                    "first reply is the first request");
    harness::expect(std::string(f2.payload.begin(), f2.payload.end()) == second,
                    "second reply is the second request");
    harness::expect(offloaded.load() == 2, "both frames were offloaded");
    server.server().stop();
}

// The queue limit is shared by every connection. One worker blocked in a task,
// one more task queued behind it, and the third submission has nowhere to go.
void case_offload_queue_limit() {
    std::printf("handler offload: the global queue limit\n");
    multi_reactor_server::config cfg = base_config();
    cfg.handler_threads   = 1;
    cfg.handler_queue_limit = 1;
    multi_reactor_server server(cfg);
    if (!server.start()) { harness::expect(false, "start() ok"); return; }

    std::atomic<bool> started{false};
    std::promise<void> gate;
    const std::shared_future<void> released = gate.get_future().share();
    const bool first = server.offload(1, [&, released] {
        started.store(true);
        released.wait();
    });
    for (int i = 0; i < 500 && !started.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    harness::expect(started.load(), "the blocking task reached a worker");

    const bool second = server.offload(2, [] {});
    const bool third  = server.offload(3, [] {});
    gate.set_value();
    server.stop();

    harness::expect(first, "the blocking task was accepted");
    harness::expect(second, "one queued task fits the limit");
    harness::expect(!third, "a task past the global limit is refused");
}

// An offloaded task that throws must not take the pool worker with it; the
// next task still runs. The exception is swallowed, as documented.
void case_offload_throwing_task() {
    std::printf("handler offload: a throwing task leaves the worker alive\n");
    multi_reactor_server::config cfg = base_config();
    cfg.handler_threads   = 1;
    cfg.handler_queue_limit = 8;
    multi_reactor_server server(cfg);
    if (!server.start()) { harness::expect(false, "start() ok"); return; }

    std::atomic<bool> ran_after{false};
    const bool threw = server.offload(1, [] { throw 1; });
    const bool next  = server.offload(2, [&] { ran_after.store(true); });
    for (int i = 0; i < 500 && !ran_after.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    server.stop();

    harness::expect(threw, "the throwing task was accepted");
    harness::expect(next, "the following task was accepted");
    harness::expect(ran_after.load(), "the worker survived the exception");
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    case_five_outcomes();
    case_accept_errors();
    case_hup_err();
    case_offload();
    case_offload_queue_limit();
    case_offload_throwing_task();
    std::printf("\n");
    return harness::finish();
}
