// verify/suites/watermark.cpp -- read-side backpressure.
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
        // Not an assertion: read_pauses already proves the pause happened, and
        // whether the kernel absorbed enough of the send buffer to resume
        // within this window is a property of the host, not of the server.
        std::printf("      frames_recv=%llu at the pause window\n",
                    (unsigned long long)ts.fixture_.stats().frames_recv.load());

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

    // ---- A2: the parser stops at the high mark -----------------------------
    //
    // Case A's drain proves the pause happened, but a kernel can absorb enough
    // of one reply to resume before the test looks. This case asks the other
    // half directly: with a backlog far larger than any local send buffer can
    // absorb (400 requests x 64 KiB replies is ~25 MiB), a server that kept
    // parsing while "paused" would have to have queued all of it. It cannot,
    // so `frames_recv < kBacklog` is a host-independent statement that the
    // parser stopped.
    {
        std::printf("A2. parsing stops at the high watermark\n");
        test_server ts(watermark_config());
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            (void)peer.connect(ts.fixture_.port());
            shrink_receive_buffer(peer);
            constexpr size_t kBacklog = 400;
            (void)send_requests(peer, kBacklog);
            harness::expect(wait_for(5000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "read side paused");
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            harness::expect(ts.fixture_.server().connection_count() == 1,
                            "the connection stayed open");
            harness::expect(ts.fixture_.stats().aborted.load() == 0,
                            "no reset");
            const uint64_t parsed = ts.fixture_.stats().frames_recv.load();
            std::printf("      parsed %llu of %zu backed-up requests\n",
                        (unsigned long long)parsed, kBacklog);
            harness::expect(parsed < kBacklog,
                            "the parser did not consume the whole backlog");
        }
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

    // ---- B2: the buffered backlog is served by the close's drain round -----
    //
    // Case B proves the close ends with FIN. It does not ask what the drain
    // round served, and the old behaviour was nothing: the high-water pause
    // reapplied at the first frame, on_input_drained saw served==0, and the
    // close parked and FIN'd over the requests already in rbuf. The pending
    // ceiling here holds every reply, so a missing answer cannot be blamed on
    // output capacity.
    {
        std::printf("B2. close while paused answers the whole backlog\n");
        multi_reactor_server::config cfg = watermark_config();
        cfg.wbuf_capacity     = 512 * 1024;
        cfg.max_pending_bytes = 512 * 1024;
        // Four 64 KiB replies at the throttled drain rate this case
        // deliberately sets up fit inside this window; the default 2s does
        // not.
        cfg.close_linger_ms   = 10000;
        constexpr size_t kBacklog = 4;
        test_server ts(cfg);
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            harness::expect(static_cast<bool>(
                                peer.connect(ts.fixture_.port())),
                            "client connected");
            shrink_receive_buffer(peer);

            const uint64_t sent_at = ts.fixture_.stats().bytes_recv.load();
            (void)send_requests(peer, kBacklog);
            const size_t want_bytes =
                sent_at + kBacklog * (industrial::FRAME_HEADER_SIZE + 1);
            harness::expect(wait_for(2000, [&] {
                                return ts.fixture_.stats().bytes_recv.load() >=
                                       want_bytes;
                            }), "the requests reached the server");
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "paused with requests still unserved");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const uint64_t served_before =
                ts.fixture_.stats().frames_recv.load();
            std::printf("      frames_recv before close: %llu of %zu\n",
                        (unsigned long long)served_before, kBacklog);
            harness::expect(served_before < kBacklog,
                            "the pause left a backlog for the close to serve");

            const auto ids = ts.fixture_.server().connection_ids();
            const uint64_t id = ids.empty() ? 0 : ids.front();
            harness::expect(id != 0, "connection id observed");
            if (id != 0) (void)ts.fixture_.server().close_connection(id);

            frame_stream in;
            in.fd = peer.fd();
            size_t replies = 0;
            harness::close_kind end = harness::close_kind::any;
            for (int i = 0; i < static_cast<int>(kBacklog) + 10; ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) { end = r.end; break; }
                if (r.type == frame_type::DATA &&
                    r.payload.size() == kReplyBytes) ++replies;
            }
            auto& st = ts.fixture_.stats();
            std::printf("      replies=%zu of %zu end=%s frames_recv=%llu "
                        "frames_sent=%llu aborted=%llu\n",
                        replies, kBacklog,
                        end == harness::close_kind::fin ? "FIN" : "other",
                        (unsigned long long)st.frames_recv.load(),
                        (unsigned long long)st.frames_sent.load(),
                        (unsigned long long)st.aborted.load());
            harness::expect(replies == kBacklog,
                            "the backlog buffered at close time was answered");
            harness::expect(end == harness::close_kind::fin,
                            "the paused close still ended with FIN");
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

    // ---- C2: a pause adds to the assembly budget, it does not reset it -----
    //
    // Case C proves a paused partial frame is not killed. It cannot tell
    // "added the pause" from "restarted the clock", because there the frame is
    // stamped immediately before the pause. Here the frame spends a long
    // non-paused interval before a second pause: a resume that re-stamped
    // would leave a full frame_assembly_timeout_ms, while adding the paused
    // intervals leaves only the remainder.
    {
        std::printf("C2. a pause adds to the assembly budget, not a fresh one\n");
        multi_reactor_server::config cfg = watermark_config();
        constexpr int kAssemblyMs  = 2000;
        constexpr int kNonPausedMs = 1200;
        cfg.frame_assembly_timeout_ms = kAssemblyMs;
        // Room for four replies, so the second pause still sees output above
        // the high mark after the kernel has taken everything it will take.
        cfg.wbuf_capacity             = 512 * 1024;
        cfg.max_pending_bytes         = 512 * 1024;
        cfg.read_high_watermark_bytes = 128 * 1024;
        cfg.read_low_watermark_bytes  = 8 * 1024;
        test_server ts(cfg);
        if (!ts.fixture_.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer peer;
            harness::expect(static_cast<bool>(peer.connect(ts.fixture_.port())),
                            "client connected");
            shrink_receive_buffer(peer);
            frame_stream in;
            in.fd = peer.fd();

            // First pause: two 64 KiB replies cross the high mark just after
            // the four-byte partial header has stamped the frame. Same shape
            // as case C.
            uint8_t first[4] = {0xEB, 0x90, 0x00, 0x02};
            (void)send_requests(peer, 2);
            (void)peer.send_raw(first, sizeof(first));
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 1;
                            }), "first pause with the frame stamped");

            // A pause longer than the budget must not fire the sweep.
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            harness::expect(
                ts.fixture_.stats().frame_assembly_timeouts.load() == 0,
                "no assembly timeout during the first pause");

            // Drain both replies; pending falls to the low mark, resume_read
            // runs, and the read side is open again with the frame incomplete.
            size_t replies = 0;
            for (int i = 0; i < 100 && replies < 2; ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) break;
                if (r.type == frame_type::DATA &&
                    r.payload.size() == kReplyBytes)
                    ++replies;
            }
            harness::expect(replies == 2, "both replies drained");

            // Spend the non-paused part of the budget.
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kNonPausedMs));

            // Queue four replies, then complete the header (declares one
            // payload byte that is never sent). The read now finds output
            // above the high mark and parks the read side a second time.
            const uint64_t id = ts.fixture_.server().connection_ids().empty()
                                    ? 0
                                    : ts.fixture_.server().connection_ids()[0];
            std::vector<uint8_t> chunk = reply_chunk();
            bool queued = id != 0;
            for (int i = 0; i < 4; ++i)
                queued = queued && ts.fixture_.server().send_to(
                              id, chunk.data(), chunk.size()) ==
                              industrial::send_result::ok;
            harness::expect(queued, "four replies queued while idle");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            uint8_t rest[4] = {0x00, 0x00, 0x00, 0x01};
            (void)peer.send_raw(rest, sizeof(rest));
            harness::expect(wait_for(3000, [&] {
                                return ts.fixture_.stats().read_pauses.load() >= 2;
                            }), "second pause with the frame still incomplete");

            // Longer than the budget again; the sweep stays out.
            std::this_thread::sleep_for(std::chrono::milliseconds(2200));
            harness::expect(
                ts.fixture_.stats().frame_assembly_timeouts.load() == 0,
                "no assembly timeout during the second pause");
            harness::expect(ts.fixture_.server().connection_count() == 1,
                            "the connection survived both pauses");

            // Drain the four replies. The resume leaves only the remainder of
            // the original budget, not a fresh one.
            size_t drained = 0;
            for (int i = 0; i < 200 && drained < 4; ++i) {
                frame_stream::result r = in.next(3000);
                if (!r.frame) break;
                if (r.type == frame_type::DATA &&
                    r.payload.size() == kReplyBytes)
                    ++drained;
            }
            harness::expect(drained == 4, "the four replies were drained");
            const auto drained_at = std::chrono::steady_clock::now();

            const bool fired = wait_for(3000, [&] {
                return ts.fixture_.stats().frame_assembly_timeouts.load() >= 1;
            });
            const auto waited =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - drained_at).count();
            std::printf("      assembly timeout fired %lld ms after the drain "
                        "(budget %d ms, non-paused %d ms)\n",
                        static_cast<long long>(waited), kAssemblyMs,
                        kNonPausedMs);
            harness::expect(fired, "the partial frame timed out after resume");
            harness::expect(waited < kAssemblyMs * 3 / 4,
                            "the non-paused time was charged, not discarded");
            harness::expect(wait_for(2000, [&] {
                                return ts.fixture_.server().connection_count() == 0;
                            }), "the timed-out connection was closed");
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
