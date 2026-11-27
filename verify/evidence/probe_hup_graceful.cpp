// verify/evidence/probe_hup_graceful.cpp
//
// A server-initiated graceful close has two halves: shutdown(SHUT_WR) sends our
// FIN, and the peer answers with its own. On Linux, once both directions are
// shut the socket reports EPOLLHUP -- usually together with EPOLLIN, because
// the read side is also at EOF.
//
// loop() used to test `EPOLLHUP | EPOLLERR` before EPOLLIN and abort on either.
// That classified the second half of every clean server close as a broken
// connection: `aborted` incremented, close_handler saw "hup/err", and any input
// the peer had already sent was dropped instead of being served.
//
// This probe performs that exact sequence with a well-behaved client and prints
// what the server concluded. Before the fix: aborted=1, reason=hup/err. After:
// aborted=0, reason=peer closed.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -O1 -Iinclude
//       verify/evidence/probe_hup_graceful.cpp -o /tmp/probe_hup_graceful
//   /tmp/probe_hup_graceful
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::multi_reactor_server;

namespace {

bool connect_to(uint16_t port, int* out) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return false;
    }
    *out = fd;
    return true;
}

bool send_request(int fd) {
    uint8_t frame[industrial::FRAME_HEADER_SIZE + 2];
    industrial::encode_frame_into(frame, industrial::frame_type::DATA, "hi", 2);
    return ::send(fd, frame, sizeof(frame), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(sizeof(frame));
}

// Reads until the server's FIN. A reset here would be a finding of its own.
bool read_until_fin(int fd) {
    uint8_t buf[512];
    for (;;) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n == 0) return true;
        if (n > 0) continue;
        if (errno == EINTR) continue;
        std::printf("recv failed: %s\n", std::strerror(errno));
        return false;
    }
}

}  // namespace

int main() {
    std::atomic<uint64_t> id{0};
    std::string reason;

    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port           = 0;
    cfg.num_reactors   = 1;
    cfg.tick_ms        = 20;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 1000;

    multi_reactor_server server(cfg);
    server.set_message_handler([&](uint64_t cid, const uint8_t*, size_t) {
        id.store(cid);
    });
    server.set_close_handler(
        [&](uint64_t, const std::string&, const char* r) {
            reason = r ? r : "(null)";
        });
    if (!server.start()) {
        std::printf("start() failed\n");
        return 2;
    }

    int fd = -1;
    if (!connect_to(server.port(), &fd)) {
        std::printf("connect failed\n");
        return 2;
    }
    if (!send_request(fd)) {
        std::printf("send failed\n");
        return 2;
    }
    for (int i = 0; i < 200 && id.load() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    if (id.load() == 0) {
        std::printf("the server never reported the connection\n");
        return 2;
    }
    const uint64_t before = server.stats().aborted.load();

    // Half one: ask the server to close, and read its FIN.
    server.close_connection(id.load());
    if (!read_until_fin(fd)) return 2;

    // Half two: answer with the client's own FIN, exactly as a peer that has
    // read everything and is done would.
    ::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const uint64_t aborted = server.stats().aborted.load() - before;
    std::printf("aborted delta=%llu reason=%s\n",
                (unsigned long long)aborted, reason.c_str());
    server.stop();

    const bool ok = aborted == 0 && reason != "hup/err";
    std::printf("%s: a clean server-initiated close stayed graceful\n",
                ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
