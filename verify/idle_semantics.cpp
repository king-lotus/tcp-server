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
//                 mid-transfer while bytesRecv is still climbing.
//
// The second one is what the file used to do: lastRecvMs was refreshed only on
// the line in parseFrames() that consumes a complete frame, so bytes that had
// arrived but had not completed a frame refreshed nothing. A 64KB frame on a
// link slower than idleTimeoutMs was closed in the middle of itself.
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
// reclaimed by the separate frameAssemblyTimeoutMs and counted as a
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

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

// 600ms idle, checked every 50ms: a transfer that keeps sending crosses the
// deadline twice over the ~1.6s each case runs, so "the deadline did not fire"
// cannot be explained by the tick simply not having come around yet.
MultiReactorServer::Config idleConfig() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;   // kernel picks; the fixture reports it
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 600;
    cfg.closeLingerMs = 1000;
    // This suite is about the idle clock. Keep the separate frame-assembly
    // deadline far away so case E can exercise it on its own.
    cfg.frameAssemblyTimeoutMs = 600000;
    return cfg;
}

// True once the server's FIN has reached the peer.
bool hungUp(int fd) {
    uint8_t probe[64];
    const ssize_t r = ::recv(fd, probe, sizeof(probe), MSG_DONTWAIT);
    return r == 0;
}

// One frame declared as kTotal bytes, delivered kChunk bytes every 150ms and
// never finished: everything a slow link does to a large frame, with the peer
// never once going quiet.
void caseHalfFrame() {
    std::printf("A. half-frame: one frame declared, dribbled, never completed\n");
    harness::Fixture server(idleConfig(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t kChunk = 2000;
    const size_t kTotal = 14000;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::writeFrameHeader(hdr, FrameType::DATA, kTotal);
    if (!peer.sendRaw(hdr, sizeof(hdr))) {
        harness::expect(false, "header sent");
        return;
    }

    bool   fin   = false;
    size_t sent  = 0;
    for (int i = 0; i < static_cast<int>(kTotal / kChunk); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> body(kChunk, 'x');
        if (!peer.sendRaw(body.data(), body.size())) break;
        sent += kChunk;
        if (hungUp(fd)) { fin = true; break; }
    }

    const unsigned long long recv = server.stats().bytesRecv.load();
    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu B, server received %llu B, idleTimeouts %llu\n",
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
void caseSilent() {
    std::printf("B. silent: connected, then nothing at all\n");
    harness::Fixture server(idleConfig(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hungUp(fd);
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      idleTimeouts %llu, server FIN'd: %s\n", idle, fin ? "yes" : "no");
    harness::expect(idle >= 1, "a silent peer is still reclaimed");
    harness::expect(fin, "and the peer sees it");

    peer.close();
}

// Same bytes, same rate, each piece a complete frame: what the clock used to
// require, kept here so narrowing it to bytes cannot be mistaken for the clock
// having stopped working.
void caseFullFrames() {
    std::printf("C. full-frames: the same pieces, each a complete frame\n");
    std::atomic<long> frames{0};
    harness::Fixture server(
        idleConfig(),
        [&frames](uint64_t, const uint8_t*, size_t) { frames.fetch_add(1); });
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t kChunk = 2000;
    bool   fin = false;
    size_t sent = 0;
    for (int i = 0; i < 7; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + kChunk);
        industrial::encodeFrameInto(f.data(), FrameType::DATA, nullptr, kChunk);
        std::memset(f.data() + industrial::FRAME_HEADER_SIZE, 'x', kChunk);
        if (!peer.sendRaw(f.data(), f.size())) break;
        sent += f.size();
        if (hungUp(fd)) { fin = true; break; }
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu B in %ld frames, idleTimeouts %llu\n",
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
void caseHalfFrameThenSilent() {
    std::printf("D. half-frame that stops mid-frame: is it reclaimed then?\n");
    harness::Fixture server(idleConfig(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }
    const int fd = peer.fd();

    const size_t kChunk = 2000;
    const size_t kTotal = industrial::MAX_FRAME_PAYLOAD;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::writeFrameHeader(hdr, FrameType::DATA, kTotal);
    if (!peer.sendRaw(hdr, sizeof(hdr))) {
        harness::expect(false, "header sent");
        return;
    }

    // All but the last byte, delivered fast enough that the clock never expires
    // while the peer is still active.
    size_t sent = 0;
    while (sent + kChunk < kTotal) {
        std::vector<uint8_t> body(kChunk, 'x');
        if (!peer.sendRaw(body.data(), body.size())) break;
        sent += kChunk;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const unsigned long long idleWhileSending = server.stats().idleTimeouts.load();

    // Now silent, inside a frame, for well past the deadline.
    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hungUp(fd);
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu of %zu bytes, then stopped; idleTimeouts %llu "
                "(was %llu while sending), FIN'd: %s\n",
                sent, kTotal, idle, idleWhileSending, fin ? "yes" : "no");

    harness::expect(idleWhileSending == 0,
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
MultiReactorServer::Config stuckFrameConfig() {
    MultiReactorServer::Config cfg = idleConfig();
    cfg.idleTimeoutMs = 5000;             // the frame deadline must win
    cfg.frameAssemblyTimeoutMs = 300;     // not the 5 x idle default
    return cfg;
}

void caseStuckFrame() {
    std::printf("E. stuck frame: bytes keep arriving, frame never completes\n");
    harness::Fixture server(stuckFrameConfig(),
                            [](uint64_t, const uint8_t*, size_t) {});
    if (!server.started()) { harness::expect(false, "start() ok"); return; }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    const size_t kTotal = 2000;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::writeFrameHeader(hdr, FrameType::DATA, kTotal);
    if (!peer.sendRaw(hdr, sizeof hdr)) {
        harness::expect(false, "header sent");
        return;
    }

    // One byte every 50ms: the peer is demonstrably alive, but the declared
    // frame cannot finish inside 300ms. Send until the far side hangs up.
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        uint8_t b = static_cast<uint8_t>(120);
        if (!peer.sendRaw(&b, 1)) break;
    }

    const harness::Status st = peer.expectClose(harness::Close::Any, 1000);
    const unsigned long long frameTimeouts =
        server.stats().frameAssemblyTimeouts.load();
    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      frameAssemblyTimeouts %llu, idleTimeouts %llu\n",
                frameTimeouts, idle);

    harness::expect(static_cast<bool>(st),
                    "the stuck frame was closed", st.why);
    harness::expect(frameTimeouts >= 1,
                    "counted as a frame assembly timeout");
    harness::expect(idle == 0, "not as an idle timeout");

    peer.close();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("idleTimeoutMs=600, tickMs=50; cases A-D run ~1.6s at the\n"
                "same byte rate. Case E sets a 300ms frame-assembly "
                "deadline.\n\n");
    caseHalfFrame();
    caseSilent();
    caseFullFrames();
    caseHalfFrameThenSilent();
    caseStuckFrame();
    std::printf("\n");
    return harness::finish();
}
