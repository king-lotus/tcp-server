// verify/send_results.cpp -- sendTo()'s five answers, and two counters, asserted.
//
// sendTo() is the whole of how an application puts bytes on a connection, and
// SendResult is the whole of what it learns back. Four of the five values had no
// assertion anywhere under verify/: no case ever asked for NotConnected,
// TooLarge, Overloaded or InvalidArgument. Correct and unguarded is a worse
// state than wrong -- any edit can change any of them and every configuration
// still comes back green, which is exactly how the round-6 review found them.
//
// Two counters were in the same position, and are covered here for the same
// reason:
//
//   Stats::acceptErrors -- the accept loop's EMFILE/ENFILE path, the one accept
//   failure that can be predicted at startup, and the one that leaves no other
//   symptom than "it stopped accepting and said nothing".
//
//   The reactor's EPOLLHUP|EPOLLERR branch -- the only place a connection is
//   closed because the kernel says the socket is already broken, rather than
//   because either side asked.
//
// What is asserted:
//   1. Each of the five SendResult values, on the state that produces it.
//      Overloaded in particular is made a property of the frame (a ceiling of
//      32 bytes) rather than of timing, so it does not have to win a race
//      against the drain to be reached.
//   2. Closed while a close is in progress: sendTo() must not promise a frame
//      the owner has already decided to refuse (R6-3), and must not leave a
//      queueDrops behind while saying Ok.
//   3. acceptErrors climbs when the descriptor ceiling is lower than the number
//      of peers knocking, instead of the loop going quiet.
//   4. hup/err, asked rather than assumed: whether this host reports an aborted
//      connection at all is checked first, because an abort that arrives as an
//      ordinary FIN never reaches the branch, and asserting against a break the
//      host cannot produce would be asserting against the host.
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
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;
using industrial::SendResult;

namespace {

// A case that asked the machine a question first and did not like the answer.
// It says which question. It is not a pass.
void skip(const char* what) {
    std::printf("  [SKIP] %s\n", what);
}

const char* name(SendResult r) {
    switch (r) {
        case SendResult::Ok:              return "Ok";
        case SendResult::NotConnected:    return "NotConnected";
        case SendResult::Closed:          return "Closed";
        case SendResult::TooLarge:        return "TooLarge";
        case SendResult::Overloaded:      return "Overloaded";
        case SendResult::InvalidArgument: return "InvalidArgument";
    }
    return "?";
}

uint16_t pickPort() {
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

MultiReactorServer::Config baseConfig() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;   // nothing here may end by a timer
    cfg.closeLingerMs = 3000;
    return cfg;
}

// ---- 1. the five answers -------------------------------------------------
void caseFiveOutcomes() {
    std::printf("sendTo(): one of each of the five answers\n");
    MultiReactorServer::Config cfg = baseConfig();
    // A ceiling low enough that "over it" is a property of the frame rather
    // than of timing: any frame larger than 32 bytes is refused on sight, so
    // Overloaded does not have to outrun the drain to be observed.
    cfg.maxPendingBytes = industrial::FRAME_HEADER_SIZE * 4;
    cfg.wbufCapacity    = industrial::FRAME_HEADER_SIZE * 64;
    harness::Fixture server(cfg, [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) { harness::expect(false, "client connected"); return; }
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    const std::vector<uint64_t> ids = server.server().connectionIds();
    if (ids.empty()) { harness::expect(false, "connection registered"); return; }
    const uint64_t id = ids[0];

    const std::string small(4, 'a');
    const std::string big(100, 'b');
    // Never issued, and never will be: ids are handed out from 1 upwards and
    // this case is the only thing running.
    const uint64_t neverIssued = 999999;

    const SendResult ok      = server.server().sendTo(id, small.data(), small.size());
    const SendResult unknown = server.server().sendTo(neverIssued, small.data(), small.size());
    const SendResult tooBig  = server.server().sendTo(id, small.data(),
                                             industrial::MAX_FRAME_PAYLOAD + 1);
    const SendResult noData  = server.server().sendTo(id, nullptr, 5);
    const SendResult over    = server.server().sendTo(id, big.data(), big.size());

    std::printf("      Ok=%s NotConnected=%s TooLarge=%s InvalidArgument=%s "
                "Overloaded=%s\n",
                name(ok), name(unknown), name(tooBig), name(noData), name(over));

    harness::expect(ok == SendResult::Ok, "a live connection: Ok");
    harness::expect(unknown == SendResult::NotConnected, "an id that was never issued: NotConnected");
    harness::expect(tooBig == SendResult::TooLarge, "a payload past MAX_FRAME_PAYLOAD: TooLarge");
    harness::expect(noData == SendResult::InvalidArgument, "data == nullptr with len > 0: InvalidArgument");
    harness::expect(over == SendResult::Overloaded, "a frame past maxPendingBytes: Overloaded");

    // The fifth value, on the state R6-3 is about: a close this caller asked
    // for. Baselines are taken after the close is in effect, so the backlog the
    // close itself refuses is not confused with the send under test.
    server.server().closeConnection(id);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    const unsigned long long qd0 = server.stats().queueDrops.load();
    const unsigned long long bd0 = server.stats().bytesDropped.load();
    harness::expect(server.server().connectionCount() == 1,
                    "the closing connection is still in the table");
    const SendResult closing = server.server().sendTo(id, small.data(), small.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::printf("      while closing: %s (queueDrops +%llu, bytesDropped +%llu)\n",
                name(closing), server.stats().queueDrops.load() - qd0,
                server.stats().bytesDropped.load() - bd0);
    harness::expect(closing == SendResult::Closed, "a connection whose close is in progress: Closed");
    harness::expect(server.stats().queueDrops.load() - qd0 == 0 &&
           server.stats().bytesDropped.load() - bd0 == 0,
           "and it bought no drops: the refusal happens before any accounting");

    peer.close();
    server.stop();
}

// ---- 2. acceptErrors ----------------------------------------------------
// The ceiling is per-process, so the server has to be the one that runs out
// while something else keeps knocking: a forked child with a lowered hard
// limit, and this process opening peers it will never close.
int serverUnderCeiling(uint16_t port) {
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
    MultiReactorServer::Config cfg;
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = port;
    cfg.numReactors     = 1;
    cfg.maxConnections  = 10000;   // the ceiling under test is descriptors, not admission
    cfg.tickMs          = 50;
    cfg.idleTimeoutMs   = 60000;
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) {
        std::printf("      child: start() failed\n");
        std::fflush(stdout);
        return 2;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    const unsigned long long acc  = server.stats().accepted.load();
    const unsigned long long errs = server.stats().acceptErrors.load();
    std::printf("      child: accepted %llu, acceptErrors %llu\n", acc, errs);
    std::fflush(stdout);
    server.stop();
    return errs > 0 ? 0 : 1;
}

void caseAcceptErrors() {
    std::printf("acceptErrors: the accept loop under a descriptor ceiling\n");
    const uint16_t port = pickPort();
    if (port == 0) { harness::expect(false, "a port to bind"); return; }

    pid_t pid = ::fork();
    if (pid == 0) ::_exit(serverUnderCeiling(port));

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    std::vector<harness::Peer> peers;
    for (int i = 0; i < 40; ++i) {
        harness::Peer p;
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
bool kernelReportsBrokenSocket() {
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

void caseHupErr() {
    std::printf("hup/err: a socket the kernel reports as already broken\n");
    if (!kernelReportsBrokenSocket()) {
        skip("this host turns an aborted connection into an ordinary FIN as far "
             "as epoll is concerned, so EPOLLHUP|EPOLLERR is never delivered "
             "and this branch cannot be reached from here");
        return;
    }

    std::atomic<int> hups{0};
    harness::Fixture server(
        baseConfig(),
        [](uint64_t, const uint8_t*, size_t) {},
        [&hups](uint64_t, const std::string&, const char* reason) {
            if (reason && std::strcmp(reason, "hup/err") == 0) hups.fetch_add(1);
        });
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) { harness::expect(false, "client connected"); return; }
    const int fd = peer.fd();
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    const std::vector<uint64_t> ids = server.server().connectionIds();
    if (ids.empty()) { harness::expect(false, "connection registered"); return; }

    // A request first, so the connection is one that has carried traffic in
    // both directions; then bytes the peer will not read; then the abort.
    if (!peer.send(FrameType::DATA, "hi", 2)) {
        harness::expect(false, "request sent");
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const std::string payload(64, 'x');
    (void)server.server().sendTo(ids[0], payload.data(), payload.size());
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

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    caseFiveOutcomes();
    caseAcceptErrors();
    caseHupErr();
    std::printf("\n");
    return harness::finish();
}
