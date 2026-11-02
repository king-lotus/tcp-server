// verify/bench_rx.cpp -- how many bytes a reactor can take off the socket and
// hand to a no-op handler, and whether more reactors buy more of it.
//
//   bench_rx <reactors> <clientThreads> <seconds>
//
// The client runs in the same process on purpose. A separate process would make
// the absolute number look better and tell us nothing about which side was the
// limit; keeping them together means the number is a floor for both halves
// combined, and the reactor-to-reactor ratio -- which is what this benchmark is
// actually for -- survives the noise.
//
// Frames are 64 bytes and are sent 64 at a time, so the syscall count does not
// dominate what is being measured.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

int connect_to(uint16_t port) {
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

}  // namespace

int main(int argc, char** argv) {
    const int reactors = argc > 1 ? std::atoi(argv[1]) : 1;
    const int threads  = argc > 2 ? std::atoi(argv[2]) : 1;
    const int seconds  = argc > 3 ? std::atoi(argv[3]) : 3;

    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.num_reactors   = reactors;
    cfg.tick_ms        = 500;
    cfg.idle_timeout_ms = 600000;

    if (const char* w = std::getenv("BENCH_WBUF_KB")) {
        const int kb = std::atoi(w);
        if (kb > 0) {
            cfg.wbuf_capacity    = static_cast<size_t>(kb) * 1024;
            cfg.max_pending_bytes = cfg.wbuf_capacity;
        }
    }
    const size_t wbuf_kb = cfg.wbuf_capacity / 1024;

    multi_reactor_server server(cfg);
    server.set_message_handler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) return 2;
    const uint16_t port = server.port();

    // The payload has to be as long as the length in the header says it is.
    // Encoding a 56-byte length out of a 2-byte string literal is a read past
    // the end of it -- and the server then parses the slack as the next frame's
    // header and closes the connection on a magic mismatch.
    const std::vector<uint8_t> payload(56, 0x5a);
    std::vector<uint8_t> frame(64);
    industrial::encode_frame_into(frame.data(), frame_type::DATA,
                                payload.data(), payload.size());
    // BENCH_BATCH is how many frames one send() carries. 64 frames of 64 bytes
    // is 4KB, and 4KB is where the syscall count stops being free: the same
    // machine's bare-wire floor goes from 1.8GB/s at 4KB to 6.6GB/s at 256KB.
    // A server that does not follow that curve is byte-bound, not syscall-bound.
    const char* batch_env = std::getenv("BENCH_BATCH");
    const int batch_frames = batch_env ? std::atoi(batch_env) : 64;
    std::vector<uint8_t> batch;
    batch.reserve(static_cast<size_t>(batch_frames) * frame.size());
    for (int i = 0; i < batch_frames; ++i) batch.insert(batch.end(), frame.begin(), frame.end());

    // Two ways to generate the load, and the difference decides what the number
    // means. In the same process the generator competes with the server for
    // cores, so the figure is a floor for both halves combined. With
    // BENCH_CLIENT_FORK=1 the clients live in a child process and the server
    // gets the machine to itself -- the only way to tell a reactor limit from a
    // generator limit.
    const char* fork_env = std::getenv("BENCH_CLIENT_FORK");
    const bool want_fork = fork_env != nullptr && fork_env[0] != '0';

    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::atomic<long long> sent{0};

    int start_pipe[2] = {-1, -1};
    pid_t child = -1;
    if (want_fork && ::pipe(start_pipe) == 0) {
        child = ::fork();
        if (child == 0) {
            ::close(start_pipe[1]);
            char go_byte = 0;
            const ssize_t got = ::read(start_pipe[0], &go_byte, 1);
            ::close(start_pipe[0]);
            if (got != 1) ::_exit(1);
            std::vector<std::thread> cs;
            for (int t = 0; t < threads; ++t) {
                cs.emplace_back([&] {
                    const int fd = connect_to(port);
                    if (fd < 0) return;
                    const auto deadline = std::chrono::steady_clock::now() +
                                          std::chrono::seconds(seconds);
                    while (std::chrono::steady_clock::now() < deadline) {
                        const ssize_t k = ::send(fd, batch.data(), batch.size(), MSG_NOSIGNAL);
                        if (k <= 0) break;
                    }
                    ::close(fd);
                });
            }
            for (auto& c : cs) c.join();
            ::_exit(0);
        }
        ::close(start_pipe[0]);
    }
    const bool forked = child > 0;

    std::vector<std::thread> clients;
    if (!forked) {
        for (int t = 0; t < threads; ++t) {
            clients.emplace_back([&] {
                const int fd = connect_to(port);
                if (fd < 0) return;
                while (!go.load()) {}
                while (!stop.load()) {
                    const ssize_t k = ::send(fd, batch.data(), batch.size(), MSG_NOSIGNAL);
                    if (k <= 0) break;
                    sent.fetch_add(k);
                }
                ::close(fd);
            });
        }
    }

    // Let the connections register before the clock starts, or the first batch
    // measures accept rather than ingest.
    const size_t want = static_cast<size_t>(threads);
    for (int i = 0; i < 400 && server.connection_count() < want; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const uint64_t before = server.stats().bytes_recv.load();
    if (forked) {
        const char go_byte = 1;
        const ssize_t wrote = ::write(start_pipe[1], &go_byte, 1);
        (void)wrote;   // the child times out on its own if this never lands
        ::close(start_pipe[1]);
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        int status = 0;
        ::waitpid(child, &status, 0);
    } else {
        go.store(true);
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        stop.store(true);
        for (auto& c : clients) c.join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    const uint64_t after = server.stats().bytes_recv.load();

    server.stop();

    const double mbps = static_cast<double>(after - before) / (1024.0 * 1024.0) /
                        static_cast<double>(seconds);
    const double kfps = static_cast<double>(after - before) / 64.0 /
                        static_cast<double>(seconds) / 1000.0;
    std::printf("ingest reactors=%d clients=%d mb_per_s=%.1f kframes_per_s=%.0f wbuf=%zu mode=%s\n",
                reactors, threads, mbps, kfps, wbuf_kb, forked ? "fork" : "same");
    return 0;
}
