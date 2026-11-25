// verify/idle_semantics.cpp -- what "idle" is measured in.
//
// An idle timeout is a promise about silence: if the peer is still sending, the
// deadline must not fire. This case pins down what counts as silence, because
// the answer is not obvious from the outside and the two candidates differ by
// exactly one situation -- a peer inside a frame it has not finished sending.
//
//   - bytes:      the deadline means "no byte arrived". A peer dribbling one
//                 large frame over a slow link is active and must be kept.
//   - frames:     the deadline means "no complete frame arrived". That same peer
//                 is idle for as long as the frame takes to arrive, and is closed
//                 mid-transfer while bytes_recv is still climbing.
//
// The second one is what the file used to do: last_recv_ms was refreshed only on
// the line in parse_frames() that consumes a complete frame, so bytes that had
// arrived but had not completed a frame refreshed nothing. A 64KB frame on a
// link slower than idle_timeout_ms was closed in the middle of itself.
//
// Three peers, all at the same byte rate, on a 600ms deadline:
//
//   A. half-frame  -- one frame declared, dribbled in pieces, never completed.
//   B. silent      -- connected and then nothing. Must be closed: this is the
//                     case the deadline exists for, and it is the guard against
//                     a fix that simply stops the timeout from firing at all.
//   C. full-frames -- the same pieces, each its own complete frame.
//
// A and C must survive; B must not.
//
// D and E cover what happens after that decision. A half-frame that stops is
// reclaimed by the idle clock. One that keeps sending is not silent, so it is
// reclaimed by the separate frame_assembly_timeout_ms and counted as a
// frame-assembly timeout, not an idle one.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/idle_semantics.cpp -o idle_semantics
//
// Exit code 0 = the deadline means no bytes arrived.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

// 600ms idle, checked every 50ms: a transfer that keeps sending crosses the
// deadline twice over the ~1.6s each case runs, so "the deadline did not fire"
// cannot be explained by the tick simply not having come around yet.
multi_reactor_server::config idle_config() {
    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port          = 0;   // kernel picks; the fixture reports it
    cfg.num_reactors   = 1;
    cfg.tick_ms        = 50;
    cfg.idle_timeout_ms = 600;
    cfg.close_linger_ms = 1000;
    // This suite is about the idle clock. Keep the separate frame-assembly
    // deadline far away so case E can exercise it on its own.
    cfg.frame_assembly_timeout_ms = 600000;
    return cfg;
}

// True once the server's FIN has reached the peer.
bool hung_up(int fd) {
    uint8_t probe[64];
    const ssize_t r = ::recv(fd, probe, sizeof(probe), MSG_DONTWAIT);
    return r == 0;
}

// One frame declared as k_total bytes, delivered k_chunk bytes every 150ms and
// never finished: everything a slow link does to a large frame, with the peer
// never once going quiet.
void case_half_frame() {
    std::printf("A. half-frame: one frame declared, dribbled, never completed\n");
    harness::fixture server(idle_config(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t k_chunk = 2000;
    const size_t k_total = 14000;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::write_frame_header(hdr, frame_type::DATA, k_total);
    if (!peer.send_raw(hdr, sizeof(hdr))) {
        harness::expect(false, "header sent");
        return;
    }

    bool   fin   = false;
    size_t sent  = 0;
    for (int i = 0; i < static_cast<int>(k_total / k_chunk); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> body(k_chunk, 'x');
        if (!peer.send_raw(body.data(), body.size())) break;
        sent += k_chunk;
        if (hung_up(fd)) { fin = true; break; }
    }

    const unsigned long long recv = server.stats().bytes_recv.load();
    const unsigned long long idle = server.stats().idle_timeouts.load();
    std::printf("      sent %zu B, server received %llu B, idle_timeouts %llu\n",
                sent, recv, idle);

    harness::expect(recv > 0, "the bytes arrived (the peer was not silent)");
    harness::expect(!fin,
                    "a peer that never stopped sending is not closed for idling");
    harness::expect(idle == 0,
                    "the idle deadline did not fire on a peer inside a frame");

    peer.close();
}

// The control: a peer that really is silent still has to be reclaimed, or the
// fix for the case above is just the timeout being switched off.
void case_silent() {
    std::printf("B. silent: connected, then nothing at all\n");
    harness::fixture server(idle_config(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hung_up(fd);
    }

    const unsigned long long idle = server.stats().idle_timeouts.load();
    std::printf("      idle_timeouts %llu, server FIN'd: %s\n", idle, fin ? "yes" : "no");
    harness::expect(idle >= 1, "a silent peer is still reclaimed");
    harness::expect(fin, "and the peer sees it");

    peer.close();
}

// Same bytes, same rate, each piece a complete frame: what the clock used to
// require, kept here so narrowing it to bytes cannot be mistaken for the clock
// having stopped working.
void case_full_frames() {
    std::printf("C. full-frames: the same pieces, each a complete frame\n");
    std::atomic<long> frames{0};
    harness::fixture server(
        idle_config(),
        [&frames](uint64_t, const uint8_t*, size_t) { frames.fetch_add(1); });
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t k_chunk = 2000;
    bool   fin = false;
    size_t sent = 0;
    for (int i = 0; i < 7; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + k_chunk);
        industrial::encode_frame_into(f.data(), frame_type::DATA, nullptr, k_chunk);
        std::memset(f.data() + industrial::FRAME_HEADER_SIZE, 'x', k_chunk);
        if (!peer.send_raw(f.data(), f.size())) break;
        sent += f.size();
        if (hung_up(fd)) { fin = true; break; }
    }

    const unsigned long long idle = server.stats().idle_timeouts.load();
    std::printf("      sent %zu B in %ld frames, idle_timeouts %llu\n",
                sent, frames.load(), idle);
    harness::expect(!fin,
                    "a peer sending complete frames is not closed for idling");
    harness::expect(frames.load() >= 5, "the frames were delivered and dispatched");
    harness::expect(idle == 0, "the idle deadline did not fire");

    peer.close();
}

// The consequence of A, and the question its fix leaves open: if a peer inside
// a frame is no longer idle-reclaimed, what reclaims it?
//
// Answer, measured rather than assumed: the same clock, the moment the peer
// actually stops. A frame it never finishes cannot grow without bound -- the
// header's length is capped at MAX_FRAME_PAYLOAD (a larger one is closed as
// "frame too large" on arrival) and rbuf is required to hold the largest legal
// frame, so "rbuf full with an incomplete frame" is unreachable under start()'s
// validation. A peer can stall inside a frame for as long as it keeps sending;
// the instant it stops, it is a silent peer again.
//
// So: send a legal maximum frame all but one byte of it, then stop talking.
void case_half_frame_then_silent() {
    std::printf("D. half-frame that stops mid-frame: is it reclaimed then?\n");
    harness::fixture server(idle_config(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t k_chunk = 2000;
    const size_t k_total = industrial::MAX_FRAME_PAYLOAD;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::write_frame_header(hdr, frame_type::DATA, k_total);
    if (!peer.send_raw(hdr, sizeof(hdr))) {
        harness::expect(false, "header sent");
        return;
    }

    // All but the last byte, delivered fast enough that the clock never expires
    // while the peer is still active.
    size_t sent = 0;
    while (sent + k_chunk < k_total) {
        std::vector<uint8_t> body(k_chunk, 'x');
        if (!peer.send_raw(body.data(), body.size())) break;
        sent += k_chunk;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const unsigned long long idle_while_sending = server.stats().idle_timeouts.load();

    // Now silent, inside a frame, for well past the deadline.
    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hung_up(fd);
    }

    const unsigned long long idle = server.stats().idle_timeouts.load();
    std::printf("      sent %zu of %zu bytes, then stopped; idle_timeouts %llu "
                "(was %llu while sending), FIN'd: %s\n",
                sent, k_total, idle, idle_while_sending, fin ? "yes" : "no");

    harness::expect(idle_while_sending == 0,
                    "still not reclaimed while it was sending");
    harness::expect(fin, "reclaimed once it went silent, even mid-frame");
    harness::expect(idle >= 1,
                    "by the idle clock, which is the only thing that changed");

    peer.close();
}

// A. and D. are about silence. A peer that keeps sending bytes but never
// completes a frame is not silent, so it needs its own deadline: this case
// makes that deadline the short one and confirms the close is counted as a
// frame-assembly timeout, not an idle one.
multi_reactor_server::config stuck_frame_config() {
    multi_reactor_server::config cfg = idle_config();
    cfg.idle_timeout_ms = 5000;             // the frame deadline must win
    cfg.frame_assembly_timeout_ms = 300;     // not the 5 x idle default
    return cfg;
}

void case_stuck_frame() {
    std::printf("E. stuck frame: bytes keep arriving, frame never completes\n");
    harness::fixture server(stuck_frame_config(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    const size_t k_total = 2000;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::write_frame_header(hdr, frame_type::DATA, k_total);
    if (!peer.send_raw(hdr, sizeof hdr)) {
        harness::expect(false, "header sent");
        return;
    }

    // One byte every 50ms: the peer is demonstrably alive, but the declared
    // frame cannot finish inside 300ms. Send until the far side hangs up.
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        uint8_t b = static_cast<uint8_t>(120);
        if (!peer.send_raw(&b, 1)) break;
    }

    const harness::status st = peer.expect_close(harness::close_kind::any, 1000);
    const unsigned long long frame_timeouts =
        server.stats().frame_assembly_timeouts.load();
    const unsigned long long idle = server.stats().idle_timeouts.load();
    std::printf("      frame_assembly_timeouts %llu, idle_timeouts %llu\n",
                frame_timeouts, idle);

    harness::expect(static_cast<bool>(st),
                    "the stuck frame was closed", st.why);
    harness::expect(frame_timeouts >= 1,
                    "counted as a frame assembly timeout");
    harness::expect(idle == 0, "not as an idle timeout");

    peer.close();
}

// F. The sweep ladder as a table.
//
// do_tick sorts every connection into one of five buckets, and the interesting
// part is not any single condition but which one wins when two are true at
// once: a connection that is closing *and* silent, or paused *and* silent, or
// paused *and* holding an unfinished frame. As a ladder of push_back +
// continue inside the walk, that could only be probed by steering a live
// reactor into the overlap and watching which counter moved.
// detail::classify_sweep answers from the connection's own state, so each
// overlap below is one call and the whole table runs without a socket.
void case_sweep_priority() {
    std::printf("F. sweep priority: which bucket wins when conditions "
                "overlap\n");
    industrial::ring_buffer wbuf, rbuf;
    industrial::connection c;
    c.fsm.attach(&wbuf, &rbuf, &c.pending_bytes);
    c.fd = 7;

    industrial::detail::sweep_ctx ctx{};
    ctx.now           = 100000;
    ctx.cutoff        = 99000;   // idle after 1s without activity
    ctx.linger        = 500;
    ctx.frame_timeout = 1000;
    ctx.low_watermark = 100;

    using industrial::detail::classify_sweep;
    using industrial::detail::sweep_action;

    // Baseline: recent input, nothing else true.
    c.last_recv_ms = ctx.now;
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::none,
                    "F1 active connection is left alone");

    // A claimed close request outranks every deadline at once. It is also the
    // only bucket that still does work (drain, then close) rather than being
    // an end of its own.
    c.fsm.closing.store(true);
    c.fsm.close_started_at_ms = ctx.now - 10000;   // past linger
    c.last_recv_ms = 0;                            // and long silent
    harness::expect(classify_sweep(c, ctx, true) == sweep_action::wanted,
                    "F2 claimed close outranks linger and idle");

    // Closing outranks idle: an idle peer is parked, a closing one is already
    // on a deadline, and reading it as idle would restart a close.
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::expired,
                    "F3 closing past linger expires, never goes idle");
    c.fsm.close_started_at_ms = ctx.now;   // inside the linger window
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::none,
                    "F4 closing inside linger is idle-proof");
    c.fsm.close_started_at_ms = 0;         // unreachable: on_graceful stamps
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::none,
                    "F5 closing without a park stamp invents no deadline");
    c.fsm.closing.store(false);

    // Resumption outranks the idle clock: a paused connection is not receiving
    // by design, so silence says nothing about it.
    c.read_paused = true;
    c.last_recv_ms = 0;
    c.pending_bytes.store(10);   // below the low mark
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::resumable,
                    "F6 paused below the low mark resumes, not idle");
    c.pending_bytes.store(500);  // still backpressured
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::idle,
                    "F7 paused but backpressured is idle when silent");

    // Drain progress is activity: a paused peer that keeps reading is not idle
    // merely because it is slow.
    c.last_drain_progress_ms = ctx.now;
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::none,
                    "F8 drain progress keeps a silent peer alive");
    c.last_drain_progress_ms = 0;
    c.read_paused = false;

    // The frame deadline applies only to a connection that is reading.
    c.last_recv_ms = ctx.now;
    c.frame_started_at_ms = ctx.now - 2000;   // past frame_timeout
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::stalled,
                    "F9 an incomplete frame past its deadline stalls");
    c.read_paused = true;
    c.pending_bytes.store(500);
    c.last_drain_progress_ms = ctx.now;
    harness::expect(classify_sweep(c, ctx, false) == sweep_action::none,
                    "F10 a paused connection is exempt from that deadline");
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("idle_timeout_ms=600, tick_ms=50; cases A-D run ~1.6s at the\n"
                "same byte rate. Case E sets a 300ms frame-assembly "
                "deadline.\n\n");
    case_half_frame();
    case_silent();
    case_full_frames();
    case_half_frame_then_silent();
    case_stuck_frame();
    case_sweep_priority();
    std::printf("\n");
    return harness::finish();
}
