// verify/watermark.cpp -- read-side backpressure.
//
// Before the watermarks, the reactor read and dispatched regardless of how
// much output was already waiting for the peer. A client that pipelined
// requests and read slowly drove pending_bytes to its ceiling, and the PING
// path turned that into "pending bytes at limit on pong" -- an RST for a peer
// whose only fault was reading slowly.
//
// The reactor now stops at a frame boundary above the high watermark, drops
// EPOLLIN so TCP pushes back on the sender, and resumes at the low mark (from
// the drain, or from the tick sweep when the release happened on another
// thread). A close overrides the pause so the FIN exchange can still finish.
//
// Cases:
//   A. pause at the high mark, no abort, then resume and answer everything;
//   B. close while paused ends with FIN, not RST;
//   C. a partial frame sitting in rbuf while paused is not killed by
//      frame_assembly_timeout_ms, and completes after resume;
//   D. a peer that half-closes while paused still gets every reply and a FIN;
//   E. idle reclaim: a paused peer that never drains is closed, while one that
//      keeps reading is not idle-killed.
//
// Linux only. Exit code 0 = all cases passed.
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <functional>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

constexpr size_t kReplyBytes = 64 * 1024;
constexpr size_t kRequests   = 40;

std::vector<uint8_t> reply_chunk() {
    return std::vector<uint8_t>(kReplyBytes, 0x5A);
}

harness::status send_requests(harness::peer& peer, size_t count) {
    std::vector<uint8_t> batch;
    batch.reserve(count * (industrial::FRAME_HEADER_SIZE + 1));
    for (size_t i = 0; i < count; ++i) {
        uint8_t frame[industrial::FRAME_HEADER_SIZE + 1];
        industrial::encode_frame_into(
            frame, frame_type::DATA, "R", 1);
        batch.insert(batch.end(), frame, frame + sizeof(frame));
    }
    return peer.send_raw(batch.data(), batch.size());
}

// A persistent frame reader -- expect_frame() drops any bytes it read past the
// frame it returned, which makes it unusable for a stream of replies.
struct frame_stream {
    int fd = -1;
    std::vector<uint8_t> pending;

    struct result {
        bool                  frame = false;
        frame_type            type  = frame_type::DATA;
        std::vector<uint8_t>  payload;
        harness::close_kind   end   = harness::close_kind::any;
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
                        (static_cast<uint16_t>(pending[2]) << 8) |
                        pending[3]);
                    out.payload.assign(
                        pending.begin() + industrial::FRAME_HEADER_SIZE,
                        pending.begin() + industrial::FRAME_HEADER_SIZE + len);
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
            if (r == 0) { out.end = harness::close_kind::fin; return out; }
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) {
                out.end = harness::close_kind::rst; return out;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(4));
                continue;
            }
            out.end = harness::close_kind::rst;
            return out;
        }
    }
};

bool wait_for(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

void shrink_receive_buffer(harness::peer& peer) {
    int rcvbuf = 16 * 1024;
    setsockopt(peer.fd(), SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
}

multi_reactor_server::config watermark_config() {
    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port           = 0;
    cfg.num_reactors   = 1;
    cfg.tick_ms        = 20;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 2000;
    cfg.rbuf_capacity  = industrial::MAX_FRAME_SIZE;
    cfg.wbuf_capacity  = 128 * 1024;
    cfg.max_pending_bytes = 128 * 1024;
    // A 64 KiB reply (65,544 bytes on the wire) crosses the high mark on its
    // own, and a loopback socket can absorb roughly 40 KiB locally before
    // sendmsg reports EAGAIN -- so after the drain the backlog is still above
    // the low mark. Without that, the pause would end before it is observable.
    cfg.socket_send_buffer_bytes = 4096;
    cfg.read_high_watermark_bytes = 64 * 1024;
    cfg.read_low_watermark_bytes  = 8 * 1024;
    return cfg;
}

// Wires the standard "answer R with a 16 KiB frame" handler and records the
// connection id. The server pointer is set after construction because the
// fixture owns the server.
struct test_server {
    std::atomic<uint64_t> id{0};
    std::atomic<multi_reactor_server*> server{nullptr};
    std::vector<uint8_t> chunk = reply_chunk();
    harness::fixture fixture_;

    explicit test_server(const multi_reactor_server::config& cfg)
        : fixture_(cfg, [this](uint64_t cid, const uint8_t* p, size_t n) {
              id.store(cid);
              if (n == 1 && p[0] == 'R') {
                  if (auto* s = server.load())
                      (void)s->send_to(cid, chunk.data(), chunk.size());
              }
          }) {
        server.store(&fixture_.server());
    }
};

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- A: pause at the high mark, resume, answer everything --------------
    {
        std::printf("A. a non-draining client is paused, not reset\n");
        test_server ts(watermark_config());
        harness::expect(ts.fixture_.started(), "start() ok");
        if (!ts.fixture_.started()) return harness::finish();

        harness::peer peer;
        harness::expect(static_cast<bool>(peer.connect(ts.fixture_.port())),
                        "client connected");
        shrink_receive_buffer(peer);
        harness::expect(static_cast<bool>(send_requests(peer, kRequests)),
                        "requests pipelined");

        harness::expect(wait_for(3000, [&] {
                            return ts.fixture_.stats().read_pauses.load() >= 1;
                        }), "read side paused at the high watermark");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        harness::expect(ts.fixture_.server().connection_count() == 1,
                        "the slow reader is still connected");
        harness::expect(ts.fixture_.stats().aborted.load() == 0,
                        "no RST was sent");
        harness::expect(ts.fixture_.stats().frames_recv.load() < kRequests,
                        "parsing stopped before consuming the whole backlog");

        frame_stream in;
        in.fd = peer.fd();
        size_t replies = 0;
        bool   timed_out = false;
        while (replies < kRequests) {
            frame_stream::result r = in.next(5000);
            if (!r.frame) { timed_out = true; break; }
            if (r.type == frame_type::DATA) {
                if (r.payload.size() != kReplyBytes) {
                    harness::expect(false, "reply has the expected size");
                    break;
                }
                ++replies;
            }
        }
        std::printf("      replies after draining: %zu of %zu%s\n",
                    replies, kRequests, timed_out ? " (timed out)" : "");
        harness::expect(!timed_out && replies == kRequests,
                        "every pipelined request was answered after resume");

        (void)peer.send(frame_type::PING, nullptr, 0);
        bool pong = false;
        for (int i = 0; i < 50 && !pong; ++i) {
            frame_stream::result r = in.next(2000);
            if (!r.frame) break;
            if (r.type == frame_type::PONG) pong = true;
        }
        harness::expect(pong, "the resumed connection answers a PING");
        harness::expect(ts.fixture_.stats().aborted.load() == 0,
                        "the exchange stayed graceful");
    }

    // ---- B: close while paused ---------------------------------------------
    {
        std::printf("B. close while paused still ends with FIN\n");
        test_server ts(watermark_config());
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            harness::expect(static_cast<bool>(
                                peer.connect(ts.fixture_.port())),
                            "client connected");
            shrink_receive_buffer(peer);
            (void)send_requests(peer, kRequests);
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "paused before the close");
            const uint64_t id = ts.fixture_.server().connection_ids().empty()
                                    ? 0
                                    : ts.fixture_.server().connection_ids()[0];
            harness::expect(id != 0, "connection id observed");
            if (id != 0) (void)ts.fixture_.server().close_connection(id);

            frame_stream in;
            in.fd = peer.fd();
            harness::close_kind end = harness::close_kind::any;
            for (int i = 0; i < 100; ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) { end = r.end; break; }
            }
            harness::expect(end == harness::close_kind::fin,
                            "the paused close ended with FIN, not RST");
            harness::expect(ts.fixture_.stats().aborted.load() == 0,
                            "the close was graceful");
        }
    }

    // ---- C: a partial frame survives the pause -----------------------------
    {
        std::printf("C. frame assembly does not fire while reads are paused\n");
        multi_reactor_server::config cfg = watermark_config();
        cfg.frame_assembly_timeout_ms = 100;
        // Two replies are queued before the pause, so the drain's local
        // buffering leaves more than the low mark in wbuf and the pause
        // outlives the assembly deadline. One reply would be drained down to
        // its last bytes and resume before the test could look.
        cfg.wbuf_capacity = 256 * 1024;
        cfg.max_pending_bytes = 192 * 1024;
        cfg.read_high_watermark_bytes = 128 * 1024;
        cfg.read_low_watermark_bytes = 8 * 1024;
        test_server ts(cfg);
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            harness::expect(static_cast<bool>(
                                peer.connect(ts.fixture_.port())),
                            "client connected");
            shrink_receive_buffer(peer);

            // Two 64 KiB replies cross the high mark; the four bytes after
            // them are the start of a PING header. parse_frames sees a short
            // frame (so it stamps the assembly deadline), then handle_read
            // pauses on the high mark.
            (void)send_requests(peer, 2);
            uint8_t half_header[4] = {0xEB, 0x90, 0x00, 0x02};
            (void)peer.send_raw(half_header, sizeof(half_header));
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "paused with a partial frame buffered");

            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            harness::expect(ts.fixture_.server().connection_count() == 1,
                            "the paused partial frame was not aborted");
            harness::expect(
                ts.fixture_.stats().frame_assembly_timeouts.load() == 0,
                "frame_assembly_timeout_ms did not fire while paused");

            // Complete the PING and drain: the assembly deadline restarts on
            // resume, and the frame is served.
            uint8_t rest[4] = {0x00, 0x00, 0x00, 0x00};
            (void)peer.send_raw(rest, sizeof(rest));
            frame_stream in;
            in.fd = peer.fd();
            size_t replies = 0;
            bool pong = false;
            for (int i = 0; i < 100 && (replies < 2 || !pong); ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) break;
                if (r.type == frame_type::DATA &&
                    r.payload.size() == kReplyBytes) ++replies;
                if (r.type == frame_type::PONG) pong = true;
            }
            harness::expect(replies == 2, "the buffered replies were served");
            harness::expect(pong, "the completed frame was served after resume");
            harness::expect(ts.fixture_.stats().aborted.load() == 0,
                            "no abort during the pause/resume");
        }
    }

    // ---- D: peer FIN while paused ------------------------------------------
    {
        std::printf("D. a peer's FIN is found when the pause is lifted\n");
        test_server ts(watermark_config());
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            harness::expect(static_cast<bool>(
                                peer.connect(ts.fixture_.port())),
                            "client connected");
            shrink_receive_buffer(peer);
            (void)send_requests(peer, kRequests);
            ::shutdown(peer.fd(), SHUT_WR);
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "paused with the peer half-closed");

            frame_stream in;
            in.fd = peer.fd();
            size_t replies = 0;
            harness::close_kind end = harness::close_kind::any;
            for (int i = 0; i < 200; ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) { end = r.end; break; }
                if (r.type == frame_type::DATA &&
                    r.payload.size() == kReplyBytes) ++replies;
            }
            std::printf("      replies=%zu end=%s\n", replies,
                        end == harness::close_kind::fin ? "FIN" : "other");
            harness::expect(replies == kRequests,
                            "every reply survived the peer's FIN");
            harness::expect(end == harness::close_kind::fin,
                            "the peer saw an orderly FIN");
            harness::expect(ts.fixture_.stats().aborted.load() == 0,
                            "the half-close was not reset");
        }
    }

    // ---- E: idle reclaim while paused --------------------------------------
    {
        std::printf("E. paused idle: still reclaimed, but not while draining\n");
        multi_reactor_server::config cfg = watermark_config();
        cfg.idle_timeout_ms = 500;
        cfg.close_linger_ms = 300;

        // E1: the peer never reads -> idle reclaim ends it.
        {
            test_server ts(cfg);
            if (!ts.fixture_.started()) {
                harness::expect(false, "start() ok");
            } else {
                harness::peer peer;
                (void)peer.connect(ts.fixture_.port());
                shrink_receive_buffer(peer);
                (void)send_requests(peer, kRequests);
                harness::expect(wait_for(3000, [&] {
                                    return ts.fixture_.stats().read_pauses.load() >= 1;
                                }), "paused before the idle window");
                harness::expect(wait_for(3000, [&] {
                                    return ts.fixture_.server().connection_count() == 0;
                                }), "a paused peer that never drains is reclaimed");
                harness::expect(ts.fixture_.stats().idle_timeouts.load() >= 1,
                                "idle_timeouts counted it");
            }
        }

        // E2: the peer keeps reading slowly -> drain progress is activity.
        {
            test_server ts(cfg);
            if (!ts.fixture_.started()) {
                harness::expect(false, "start() ok");
            } else {
                harness::peer peer;
                (void)peer.connect(ts.fixture_.port());
                shrink_receive_buffer(peer);
                (void)send_requests(peer, kRequests);
                harness::expect(wait_for(3000, [&] {
                                    return ts.fixture_.stats().read_pauses.load() >= 1;
                                }), "paused before the slow drain");

                const auto until = std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(1500);
                while (std::chrono::steady_clock::now() < until) {
                    uint8_t sink[16 * 1024];
                    (void)::recv(peer.fd(), sink, sizeof(sink), MSG_DONTWAIT);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(50));
                }
                harness::expect(ts.fixture_.server().connection_count() == 1,
                                "a paused peer that keeps draining is not idle-killed");
                harness::expect(ts.fixture_.stats().aborted.load() == 0,
                                "the slow drain stayed graceful");
            }
        }
    }

    return harness::finish();
}
