// EMFILE probe: does a client hang when the accept loop runs out of
// descriptors?
//
// Shape: fork first (single-threaded), then the parent lowers RLIMIT_NOFILE and
// runs the server while the child connects from its own descriptor table. The
// server greets every accepted connection, so "greeted" separates "accepted"
// from "stuck in the backlog". The last connection opened while exhausted is the
// one measured: before the spare-descriptor change it blocked until the client's
// own timeout, after it is reset immediately.
//
// It doubles as the reproduction vehicle for the ASan+UBSan vptr report in
// teardown_closed() -- see verify/evidence/repro_ubsan_vptr.sh.
//
// Shape: fork first (single-threaded), then the parent lowers RLIMIT_NOFILE and
// runs the server while the child connects from its own descriptor table. The
// server greets every accepted connection, so "greeted" separates "accepted"
// from "stuck in the backlog".
#include "tcp-server-mt.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using industrial::multi_reactor_server;

namespace {

int g_port = 38917;   // overridable so several instances can run at once
constexpr int kRlimit = 64;
constexpr int kClientSockets = 200;

struct client_report {
    int       connects_ok;
    int       connects_failed;
    int       greeted;
    int       closed_by_peer;
    int       silent;
    int       reset;
    int       victim_found;
    long long wait_ms;
    int       wait_errno;
    int       recovered;
};

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

void run_client(int wfd) {
    client_report rep{};

    // Wait for the listener. The parent starts the server after the fork.
    int probe = -1;
    for (int i = 0; i < 500 && probe < 0; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons(g_port);
        ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) == 0)
            probe = fd;
        else
            ::close(fd);
        // The parent starts the server after the fork; under a slow runtime
        // (valgrind, a loaded box) 500 immediate attempts can all land before
        // the listener exists, which looks like "the server refused everything".
        if (probe < 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (probe < 0) {
        ssize_t w = ::write(wfd, &rep, sizeof rep); (void)w;
        _exit(0);
    }
    ::close(probe);   // hand the fd back; the server sees a FIN and reaps it

    std::vector<int> socks;
    for (int i = 0; i < kClientSockets; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) break;
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons(g_port);
        ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) {
            ++rep.connects_failed;
            ::close(fd);
            break;
        }
        ++rep.connects_ok;
        socks.push_back(fd);
    }

    // Let every connection the server could accept be greeted.
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    // Classify by what the socket actually yields, not by poll flags: a reset
    // connection also polls readable, which is what made the first version call
    // 200 unserved connections "greeted".
    for (int fd : socks) {
        int fl = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, fl | O_NONBLOCK);
        char b[16];
        const ssize_t r = ::recv(fd, b, sizeof b, 0);
        if (r > 0) {
            ++rep.greeted;
        } else if (r == 0) {
            ++rep.closed_by_peer;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            ++rep.silent;
        } else {
            ++rep.reset;
        }
        ::fcntl(fd, F_SETFL, fl);
    }

    // One more connection while the process is still out of descriptors: this
    // is the measurement C1 cares about, and it is the same shape as the
    // baseline run (which blocked the full 3 s).
    int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    if (victim >= 0) {
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons(g_port);
        ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        if (::connect(victim, reinterpret_cast<sockaddr*>(&a), sizeof a) == 0) {
            rep.victim_found = 1;
            struct timeval tv{3, 0};
            ::setsockopt(victim, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
            char b[16];
            const long long t0 = now_ms();
            const ssize_t r = ::recv(victim, b, sizeof b, 0);
            rep.wait_ms    = now_ms() - t0;
            rep.wait_errno = r < 0 ? errno : 0;
        }
    }

    // C3: hand descriptors back and prove the listener recovers rather than
    // staying wedged in the exhausted branch.
    for (size_t i = 0; i < 10 && i < socks.size(); ++i) {
        ::close(socks[i]);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    int again = ::socket(AF_INET, SOCK_STREAM, 0);
    if (again >= 0) {
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons(g_port);
        ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        if (::connect(again, reinterpret_cast<sockaddr*>(&a), sizeof a) == 0) {
            struct timeval tv{1, 0};
            ::setsockopt(again, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
            char b[16];
            rep.recovered = ::recv(again, b, sizeof b, 0) > 0 ? 1 : 0;
        }
        ::close(again);
    }

    ssize_t w = ::write(wfd, &rep, sizeof rep); (void)w;
    _exit(0);
}

double cpu_ms() {
    struct rusage ru{};
    if (::getrusage(RUSAGE_SELF, &ru) != 0) return -1;
    return (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000.0 +
           (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1000.0;
}

int fd_count() {
    std::FILE* p = ::popen("ls /proc/self/fd | wc -l", "r");
    if (!p) return -1;
    int n = -1;
    if (std::fscanf(p, "%d", &n) != 1) n = -1;
    ::pclose(p);
    return n;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc > 1) g_port = std::atoi(argv[1]);
    int fds[2];
    if (::pipe(fds) != 0) { std::printf("pipe failed\n"); return 1; }

    const pid_t child = ::fork();
    if (child == 0) {
        ::close(fds[0]);
        run_client(fds[1]);
        _exit(0);
    }
    ::close(fds[1]);

    struct rlimit rl{kRlimit, kRlimit};
    if (::setrlimit(RLIMIT_NOFILE, &rl) != 0)
        std::printf("setrlimit failed: %s\n", std::strerror(errno));

    multi_reactor_server::config cfg;
    cfg.bind_addr       = "127.0.0.1";
    cfg.port            = g_port;
    cfg.num_reactors    = 2;
    cfg.max_connections = 200;      // above the descriptor budget on purpose
    cfg.tick_ms         = 100;
    cfg.idle_timeout_ms = 60000;

    multi_reactor_server server(cfg);
    server.set_message_handler([](uint64_t, const uint8_t*, size_t) {});
    server.set_conn_state_handler([&server](uint64_t cid, const std::string&,
                                            bool open) {
        if (open) server.send_to(cid, "x", 1);
    });
    if (!server.start()) { std::printf("start failed\n"); return 1; }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const double cpu0 = cpu_ms();
    const unsigned long long err0 = server.stats().accept_errors.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    const double cpu1 = cpu_ms();
    const unsigned long long err1 = server.stats().accept_errors.load();

    const auto& st = server.stats();
    std::printf("server : rlimit=%d fds=%d cpu_ms_per_s=%.1f accept_errors_per_s=%llu\n",
                kRlimit, fd_count(), cpu1 - cpu0, err1 - err0);
    std::printf("server : accepted=%llu rejected=%llu closed=%llu "
                "accept_errors=%llu fd_drops=%llu fd_system=%llu open=%zu\n",
                (unsigned long long)st.accepted.load(),
                (unsigned long long)st.rejected.load(),
                (unsigned long long)st.closed.load(),
                (unsigned long long)st.accept_errors.load(),
                (unsigned long long)st.fd_exhausted_drops.load(),
                (unsigned long long)st.fd_exhausted_system.load(),
                server.connection_count());

    int status = 0;
    ::waitpid(child, &status, 0);
    client_report rep{};
    const ssize_t n = ::read(fds[0], &rep, sizeof rep);
    if (n == static_cast<ssize_t>(sizeof rep)) {
        std::printf("client : connects_ok=%d failed=%d greeted=%d "
                    "closed=%d reset=%d silent=%d\n",
                    rep.connects_ok, rep.connects_failed, rep.greeted,
                    rep.closed_by_peer, rep.reset, rep.silent);
        if (rep.victim_found) {
            std::printf("client : victim recv took %lld ms, errno=%d (%s)\n",
                        rep.wait_ms, rep.wait_errno,
                        rep.wait_errno ? std::strerror(rep.wait_errno) : "none");
        } else {
            std::printf("client : no silent connection; every connect was served\n");
        }
        std::printf("client : recovered after freeing fds = %s\n",
                    rep.recovered ? "yes" : "NO");
    } else {
        std::printf("client : no report (%zd bytes)\n", n);
    }

    // The probe is also a CI assertion, not just evidence to read by hand.
    // A host that never reached EMFILE cannot judge the spare path: say so
    // instead of reporting a pass, the same way the suite treats [SKIP].
    int failures = 0;
    auto check = [&](bool ok, const char* what) {
        std::printf("   [%s] %s\n", ok ? "PASS" : "FAIL", what);
        if (!ok) ++failures;
    };
    const unsigned long long drops = st.fd_exhausted_drops.load();
    if (drops == 0) {
        std::printf("   [SKIP] EMFILE was not reached; this host cannot "
                    "exercise the spare path\n");
    } else {
        check(rep.victim_found != 0,
              "a client could still connect while descriptors were exhausted");
        check(rep.wait_errno == ECONNRESET,
              "the victim got ECONNRESET instead of waiting");
        check(rep.wait_ms < 1000,
              "the reset arrived promptly, not after the client's timeout");
        check(drops == st.rejected.load(),
              "every drop is accounted for as a rejection");
        check(rep.silent == 0,
              "no connection was left with no answer at all");
        check(rep.recovered != 0,
              "the listener accepted again after descriptors were freed");
    }

    server.stop();
    if (drops > 0)
        std::printf("   emfile probe: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
