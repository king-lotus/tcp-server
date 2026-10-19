// verify/bench_floor.cpp -- the wire without the library.
//
// bench_rx measures this server. This measures the same shape of traffic with
// nothing in between: accept one connection, recv into a buffer, throw the
// bytes away. Whatever it reports is the ceiling any userspace server on this
// machine is working against, so a bench_rx number that lands close to it is
// the machine talking, not the header.
//
//   bench_floor <bytes-per-send> <seconds>
//
// The client runs in the same process on purpose, matching bench_rx's default:
// both sides paying for their own cores is part of what is being compared.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

int main(int argc, char** argv) {
    const int batchBytes = argc > 1 ? std::atoi(argv[1]) : 4096;
    const int seconds    = argc > 2 ? std::atoi(argv[2]) : 3;
    // How much one recv() asks for. The server under test reads through a 16KB
    // stack buffer, so the interesting question is whether that alone explains
    // why a 64KB send is served at a third of the speed this loop manages.
    const int recvBytes  = argc > 3 ? std::atoi(argv[3]) : 64 * 1024;
    // blocking | epoll. The server under test drives its sockets the other way:
    // non-blocking, level-triggered epoll, one recv loop per wakeup. Same bytes,
    // one more syscall per batch -- this is what that model costs, measured
    // without the header in the way.
    const bool useEpoll = argc > 4 && std::strcmp(argv[4], "epoll") == 0;

    const int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) return 2;
    int one = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = 0;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(srv, (struct sockaddr*)&sa, sizeof sa) < 0) return 2;
    if (::listen(srv, 128) < 0) return 2;

    socklen_t slen = sizeof sa;
    ::getsockname(srv, (struct sockaddr*)&sa, &slen);
    const uint16_t port = ntohs(sa.sin_port);

    std::atomic<uint64_t> bytes{0};
    std::atomic<bool> stop{false};

    std::thread server([&] {
        const int c = ::accept(srv, nullptr, nullptr);
        if (c < 0) return;
        std::vector<char> buf(static_cast<size_t>(recvBytes));

        if (!useEpoll) {
            while (!stop.load()) {
                const ssize_t n = ::recv(c, buf.data(), buf.size(), 0);
                if (n <= 0) break;
                bytes.fetch_add(static_cast<uint64_t>(n));
            }
            ::close(c);
            return;
        }

        int fl = ::fcntl(c, F_GETFL, 0);
        ::fcntl(c, F_SETFL, fl | O_NONBLOCK);
        const int ep = ::epoll_create1(0);
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = c;
        ::epoll_ctl(ep, EPOLL_CTL_ADD, c, &ev);
        struct epoll_event evs[16];
        while (!stop.load()) {
            if (::epoll_wait(ep, evs, 16, 100) <= 0) continue;
            for (;;) {
                const ssize_t n = ::recv(c, buf.data(), buf.size(), 0);
                if (n > 0) {
                    bytes.fetch_add(static_cast<uint64_t>(n));
                    continue;
                }
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
                ::close(c);
                ::close(ep);
                return;
            }
        }
        ::close(c);
        ::close(ep);
    });

    std::thread client([&] {
        const int c = ::socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in peer = sa;
        peer.sin_port = htons(port);
        if (::connect(c, (struct sockaddr*)&peer, sizeof peer) < 0) {
            ::close(c);
            return;
        }
        std::vector<char> payload(static_cast<size_t>(batchBytes), 0x5a);
        while (!stop.load()) {
            if (::send(c, payload.data(), payload.size(), MSG_NOSIGNAL) <= 0) break;
        }
        ::close(c);
    });

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop.store(true);
    client.join();
    server.join();
    ::close(srv);

    const double mbps = static_cast<double>(bytes.load()) / (1024.0 * 1024.0) /
                        static_cast<double>(seconds);
    std::printf("floor batch=%d recv=%d mode=%s mb_per_s=%.1f\n", batchBytes,
                recvBytes, useEpoll ? "epoll" : "blocking", mbps);
    return 0;
}
