// verify/bench_send.cpp -- round-trip cost of the reply path, in microseconds.
//
//   bench_send <messagesPerClient> <payloadBytes> [clients] [reactors]
//
// Each client keeps one frame in flight: send a DATA frame, wait for the
// handler's echo, then send the next. With one client that is latency. With
// several clients it is also a scaling claim: the handler echo takes sendTo()'s
// owner-thread fast path (straight into wbuf), while application-thread sends
// still take the queue. The clients/reactors ratio shows whether the reply
// path scales, and the multi-client mode exists to expose the locks and
// statistics that the fast path bypasses.
//
// The clients run in this process on purpose. A separate process would make the
// absolute number look better and tell us nothing about which side was the
// limit; keeping them together means the number is a floor for both halves
// combined, and the ratio across configurations survives the noise.
//
// Reported: p50/p99/mean in microseconds over every measured round trip, plus
// the aggregate rate. p99 is the point of the exercise: a mean hides exactly
// the wakeup tail that the socket-readiness path produces.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <numeric>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::FRAME_HEADER_SIZE;
using industrial::MultiReactorServer;

namespace {

std::atomic<MultiReactorServer*> gServer{nullptr};
std::atomic<int> gPayload{56};

int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool readExactly(int fd, void* p, size_t n) {
    uint8_t* b = static_cast<uint8_t*>(p);
    size_t off = 0;
    while (off < n) {
        const ssize_t k = ::recv(fd, b + off, n - off, 0);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

struct ClientResult {
    std::vector<uint64_t> ns;
    size_t sent = 0;
    bool   connected = false;
};

void runClient(int messages, const std::vector<uint8_t>& payload,
               uint16_t port, std::atomic<int>* ready,
               std::atomic<bool>* go, ClientResult* result) {
    std::vector<uint8_t> frame(FRAME_HEADER_SIZE + payload.size());
    industrial::encodeFrameInto(frame.data(), FrameType::DATA,
                                payload.data(), payload.size());
    std::vector<uint8_t> reply(frame.size());

    const int fd = connectTo(port);
    if (fd < 0) {
        ready->fetch_add(1, std::memory_order_release);
        return;
    }
    result->connected = true;

    // Warm up: the first exchanges pay for registration and for the page faults
    // behind both buffers, and would otherwise land in p99. The payload must be
    // as long as the header claims: a shorter literal with a longer declared
    // length is a read past its end, and the bytes after it get parsed as the
    // next frame's header.
    for (int i = 0; i < 100; ++i) {
        if (::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(frame.size())) break;
        if (!readExactly(fd, reply.data(), frame.size())) break;
    }

    // Start every client's measured loop at the same point. Without this the
    // first client can finish before the last one has warmed up, and the
    // aggregate rate stops describing a concurrent run.
    ready->fetch_add(1, std::memory_order_release);
    while (!go->load(std::memory_order_acquire)) std::this_thread::yield();

    result->ns.reserve(static_cast<size_t>(messages));
    for (int i = 0; i < messages; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        if (::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(frame.size())) break;
        if (!readExactly(fd, reply.data(), frame.size())) break;
        const auto t1 = std::chrono::steady_clock::now();
        result->ns.push_back(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
                .count()));
        ++result->sent;
    }

    ::close(fd);
}

}  // namespace

int main(int argc, char** argv) {
    const int messages = argc > 1 ? std::atoi(argv[1]) : 2000;
    gPayload.store(argc > 2 ? std::atoi(argv[2]) : 56);
    const int clients = argc > 3 ? std::atoi(argv[3]) : 1;
    const int reactors = argc > 4 ? std::atoi(argv[4]) : 1;
    if (messages <= 0 || gPayload.load() <= 0 || clients <= 0 ||
        reactors <= 0) {
        std::printf("usage: bench_send <messages> <payload> [clients] "
                    "[reactors]\n");
        return 2;
    }

    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.numReactors   = reactors;
    cfg.tickMs        = 500;
    cfg.idleTimeoutMs = 600000;

    // Same knob as bench_rss: the write buffer sizes the reply path, so it has
    // to move with it or the send numbers stop being comparable.
    if (const char* w = std::getenv("BENCH_WBUF_KB")) {
        const int kb = std::atoi(w);
        if (kb > 0) {
            cfg.wbufCapacity    = static_cast<size_t>(kb) * 1024;
            cfg.maxPendingBytes = cfg.wbufCapacity;
        }
    }

    MultiReactorServer server(cfg);
    gServer.store(&server);

    // Echo: the point is to pay for a reply, not to compute one.
    server.setMessageHandler([](uint64_t id, const uint8_t* p, size_t n) {
        MultiReactorServer* s = gServer.load();
        if (s) (void)s->sendTo(id, p, n);
    });
    if (!server.start()) return 2;

    const std::vector<uint8_t> payload(static_cast<size_t>(gPayload.load()),
                                       0x5a);
    std::vector<ClientResult> results(static_cast<size_t>(clients));
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(clients));
    for (int i = 0; i < clients; ++i) {
        threads.emplace_back(runClient, messages, std::cref(payload),
                             server.port(), &ready, &go, &results[i]);
    }
    while (ready.load(std::memory_order_acquire) < clients)
        std::this_thread::yield();
    const auto wall0 = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();
    const auto wall1 = std::chrono::steady_clock::now();

    server.stop();

    std::vector<uint64_t> ns;
    for (const auto& r : results) {
        if (!r.connected || r.sent != static_cast<size_t>(messages)) {
            std::printf("send messages=%zu TOO FEW (client failed)\n",
                        ns.size());
            return 1;
        }
        ns.insert(ns.end(), r.ns.begin(), r.ns.end());
    }
    if (ns.size() < 100) {
        std::printf("send messages=%zu TOO FEW\n", ns.size());
        return 1;
    }
    std::sort(ns.begin(), ns.end());
    const double p50 = static_cast<double>(ns[ns.size() / 2]) / 1000.0;
    const double p99 = static_cast<double>(ns[ns.size() * 99 / 100]) / 1000.0;
    const uint64_t total =
        std::accumulate(ns.begin(), ns.end(), uint64_t{0});
    const double mean =
        static_cast<double>(total) / ns.size() / 1000.0;
    const double seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            wall1 - wall0).count();
    const double rate = seconds > 0 ? ns.size() / seconds : 0.0;
    std::printf("send messages=%zu payload=%d clients=%d reactors=%d "
                "p50_us=%.1f p99_us=%.1f mean_us=%.1f msg_per_s=%.0f "
                "wbuf=%zu\n",
                ns.size(), gPayload.load(), clients, reactors, p50, p99,
                mean, rate, cfg.wbufCapacity / 1024);
    return 0;
}
