// verify/evidence/probe_close_while_paused.cpp
//
// What a graceful close answers when the connection was read-paused.
//
// The close budget (`close_input_bytes`) is measured once, from rbuf occupancy
// at the moment the request is dequeued. Input that is still in the kernel
// receive buffer at that point is not part of the budget: the drain resumes
// reads, so its first round does pull some of it in, but nothing promises the
// rest. This probe measures where that line actually falls, because the header
// now documents it and a documented number with no measurement behind it is the
// kind of claim this project stopped writing down years of rounds ago.
//
// Sequence:
//   1. one request whose reply is 64 KiB, against a 4 KiB socket send buffer
//      and a peer that does not read: pending output crosses the 64 KiB high
//      watermark and the reactor drops EPOLLIN. rbuf is empty -- the pause is
//      applied at a frame boundary, after parsing -- so the close budget will
//      be 0.
//   2. the peer then writes kRequests small requests. The server is not
//      reading, so they sit in the kernel receive buffer.
//   3. close_connection(id), then the peer drains everything and counts the
//      answers before the FIN.
//
// Expected shape, and what the run prints: some answers (the drain's resume
// pulls a read budget out of the kernel), far fewer than were sent, and an
// orderly FIN rather than an RST. Before the read pause existed the same
// sequence would have answered everything, because the reactor would have read
// the backlog as it arrived.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -O1 -Iinclude
//       verify/evidence/probe_close_while_paused.cpp -o /tmp/probe_close_while_paused
//   /tmp/probe_close_while_paused
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

constexpr size_t kReplyBytes = 64 * 1024;
constexpr size_t kRequests   = 4000;

bool wait_for(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// Persistent frame reader: a stream of replies cannot be read with a
// read-one-frame helper that drops whatever it read past the frame.
struct frame_stream {
    int fd = -1;
    std::vector<uint8_t> pending;

    struct result {
        bool                 frame = false;
        frame_type           type  = frame_type::DATA;
        size_t               payload = 0;
        bool                 fin = false;
        bool                 rst = false;
    };

    result next(int timeout_ms) {
        result out;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (;;) {
            if (pending.size() >= industrial::FRAME_HEADER_SIZE) {
                const uint32_t len = (static_cast<uint32_t>(pending[4]) << 24)
                                   | (static_cast<uint32_t>(pending[5]) << 16)
                                   | (static_cast<uint32_t>(pending[6]) <<  8)
                                   |  static_cast<uint32_t>(pending[7]);
                if (pending.size() >= industrial::FRAME_HEADER_SIZE + len) {
                    out.frame = true;
                    out.type = static_cast<frame_type>(
                        (static_cast<uint16_t>(pending[2]) << 8) | pending[3]);
                    out.payload = len;
                    pending.erase(
                        pending.begin(),
                        pending.begin() + industrial::FRAME_HEADER_SIZE + len);
                    return out;
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) return out;
            uint8_t tmp[16 * 1024];
            const ssize_t r = ::recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
            if (r > 0) {
                pending.insert(pending.end(), tmp, tmp + r);
                continue;
            }
            if (r == 0) { out.fin = true; return out; }
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) { out.rst = true; return out; }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(4));
                continue;
            }
            out.rst = true;
            return out;
        }
    }
};

// SO_RCVBUF is set before connect on purpose: applied after it, the kernel has
// already chosen the window, and a 64 KiB reply can sit in the peer's buffer
// unread -- which is exactly the state this probe needs to avoid, because a
// drained reply resumes the read side and the pause then answers everything.
int connect_to(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int rcvbuf = 2048;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool send_all(int fd, const std::vector<uint8_t>& bytes) {
    size_t sent = 0;
    while (sent < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + sent, bytes.size() - sent,
                                 MSG_NOSIGNAL);
        if (n > 0) { sent += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

std::vector<uint8_t> one_request(char tag, size_t count) {
    std::vector<uint8_t> batch;
    batch.reserve(count * (industrial::FRAME_HEADER_SIZE + 1));
    for (size_t i = 0; i < count; ++i) {
        uint8_t frame[industrial::FRAME_HEADER_SIZE + 1];
        industrial::encode_frame_into(frame, frame_type::DATA, &tag, 1);
        batch.insert(batch.end(), frame, frame + sizeof(frame));
    }
    return batch;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    multi_reactor_server::config cfg;
    cfg.bind_addr       = "127.0.0.1";
    cfg.port            = 0;
    cfg.num_reactors    = 1;
    cfg.tick_ms         = 20;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 10000;
    cfg.wbuf_capacity     = 128 * 1024;
    cfg.max_pending_bytes = 128 * 1024;
    // A small kernel send buffer is what keeps the 64 KiB reply outstanding
    // once the peer stops reading; the watermark then holds the pause instead
    // of letting it flap (the first attempt at 8 KiB let the whole reply fit in
    // the doubled SO_SNDBUF, so the read side resumed and re-paused eleven
    // times and ended up answering everything).
    cfg.socket_send_buffer_bytes = 4096;
    cfg.read_high_watermark_bytes = 64 * 1024;
    cfg.read_low_watermark_bytes  = 8 * 1024;

    std::vector<uint8_t> big(kReplyBytes, 0x5A);
    std::vector<uint8_t> small_one{'q'};
    multi_reactor_server server(cfg);
    server.set_message_handler([&](uint64_t id, const uint8_t* p, size_t n) {
        if (n != 1) return;
        if (p[0] == 'P') (void)server.send_to(id, big.data(), big.size());
        else             (void)server.send_to(id, small_one.data(), 1);
    });
    if (!server.start()) {
        std::printf("start() failed\n");
        return 2;
    }

    const int fd = connect_to(server.port());
    if (fd < 0) {
        std::printf("connect failed\n");
        return 2;
    }

    // 1. One big reply, peer not reading: this is what pauses the read side.
    if (!send_all(fd, one_request('P', 1))) {
        std::printf("send P failed\n");
        return 2;
    }
    const bool paused = wait_for(3000, [&] {
        return server.stats().read_pauses.load() >= 1;
    });
    const auto ids = server.connection_ids();
    const uint64_t id = ids.empty() ? 0 : ids.front();
    std::printf("paused=%d id=%llu frames_recv=%llu\n", paused ? 1 : 0,
                static_cast<unsigned long long>(id),
                static_cast<unsigned long long>(server.stats().frames_recv.load()));
    if (!paused || id == 0) {
        std::printf("the connection never paused; nothing to measure\n");
        ::close(fd);
        server.stop();
        return 2;
    }

    // 2. The requests that will sit in the kernel buffer, unread.
    if (!send_all(fd, one_request('Q', kRequests))) {
        std::printf("send Q batch failed\n");
        ::close(fd);
        server.stop();
        return 2;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const uint64_t recv_before_close = server.stats().frames_recv.load();

    // 3. Close, and let the reactor spend its close round while the peer is
    // still not reading. `draining_input` exempts that round from the output
    // watermark, so it serves one read budget (256 frames here) rather than
    // stopping at the watermark. The ordinary continuation that follows may
    // serve more before the park lands. Once the park sets `closing`, the high
    // watermark no longer applies: the remaining input is parsed and handed to
    // the handler (its replies are refused) until the peer's FIN or
    // close_linger_ms.
    (void)server.close_connection(id);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const uint64_t recv_after_park = server.stats().frames_recv.load();

    // 4. Now read: that drains wbuf, which lets the parked close finish.
    frame_stream in;
    in.fd = fd;
    size_t answered = 0, big_replies = 0, other = 0;
    bool   fin = false, rst = false;
    for (int i = 0; i < static_cast<int>(kRequests) + 64; ++i) {
        frame_stream::result r = in.next(10000);
        if (r.fin) { fin = true; break; }
        if (r.rst) { rst = true; break; }
        if (!r.frame) break;
        if (r.type != frame_type::DATA) { ++other; continue; }
        if (r.payload == kReplyBytes) ++big_replies;
        else if (r.payload == 1) ++answered;
        else ++other;
    }

    const auto& st = server.stats();
    std::printf(
        "sent=%zu answers=%zu big=%zu other=%zu fin=%d rst=%d\n"
        "frames_recv: before close=%llu after the close round=%llu "
        "after=%llu  frames_sent=%llu\n"
        "frames_dropped_on_close=%llu bytes_dropped=%llu aborted=%llu "
        "read_pauses=%llu\n",
        kRequests, answered, big_replies, other, fin ? 1 : 0, rst ? 1 : 0,
        static_cast<unsigned long long>(recv_before_close),
        static_cast<unsigned long long>(recv_after_park),
        static_cast<unsigned long long>(st.frames_recv.load()),
        static_cast<unsigned long long>(st.frames_sent.load()),
        static_cast<unsigned long long>(st.frames_dropped_on_close.load()),
        static_cast<unsigned long long>(st.bytes_dropped.load()),
        static_cast<unsigned long long>(st.aborted.load()),
        static_cast<unsigned long long>(st.read_pauses.load()));

    // Answers, not frames_recv: parsing and answering diverge here. Every
    // request is taken in and handed to the handler -- frames_recv accounts for
    // all of them -- but once the close has parked, submit_send refuses the
    // replies, so the peer receives a prefix and a FIN. That divergence is the
    // whole finding: "the requests were handled" is not "the requests were
    // answered", and only the second one is visible on the wire.
    const bool bounded = answered > 0 && answered < kRequests;
    std::printf("%s: the close answered %zu of %zu, then FIN\n",
                bounded && fin && !rst ? "PASS" : "FAIL", answered, kRequests);
    std::printf("      parsed=%llu answered=%zu: the rest were handled and "
                "answered into nothing\n",
                static_cast<unsigned long long>(recv_after_park -
                                                recv_before_close),
                answered);
    ::close(fd);
    server.stop();
    return bounded && fin && !rst ? 0 : 1;
}
