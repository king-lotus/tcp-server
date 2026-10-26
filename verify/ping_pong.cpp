// verify/ping_pong.cpp -- the read path's most conditional branch, executed.
//
// parseFrames() answers PING itself: a reservation CAS loop against
// maxPendingBytes, a refusal that tears the connection down, and a "we are
// already closing" case that drops the reply and counts it apart. Nothing under
// verify/ ever ran any of it -- grep found no PING and no PONG in the whole
// suite -- so the branch was correct and unguarded, which is a worse state than
// being wrong: it could be broken by any edit and every configuration would
// still come back green.
//
// What is asserted, and why each one is the point rather than a detail:
//
//   1. A PING on a healthy connection gets exactly one PONG, and the PONG is a
//      frame the far side's own parser accepts -- type PONG, payload empty.
//   2. The PONG is counted like any other frame. framesSent and bytesSent are
//      reconcilable only if every frame that leaves is counted, and a PONG is a
//      frame that leaves: bytesSent must be 8 * PONG, 8 being the header.
//   3. A PING that lands after the close was asked for is dropped and counted
//      as framesDroppedOnClose. Answering it would put a frame out past the FIN,
//      which is the thing the whole close sequence exists to prevent.
//
// The client half now goes through verify/harness.h: this file used to carry
// its own pickPort/connectTo/sendAll/frame/readFrame, like nine others.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread -Iinclude verify/ping_pong.cpp -o ping_pong
//
// Exit code 0 = the PING branch answers, counts, and stays behind the FIN.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

#include "harness.h"

using industrial::FrameType;

namespace {

void caseHealthy() {
    std::printf("healthy: five PINGs on a connection that is not going anywhere\n");
    harness::Fixture server(harness::quietConfig());
    if (!server.started()) {
        harness::expect(false, "start() ok");
        return;
    }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    const int kPings = 5;
    int       got = 0;
    int       malformed = 0;
    for (int i = 0; i < kPings; ++i) {
        if (!peer.send(FrameType::PING, nullptr, 0)) break;
        harness::Frame f;
        if (!peer.expectFrame(&f, 500)) continue;
        if (f.type == FrameType::PONG && f.payload.empty()) ++got;
        else ++malformed;
    }

    // Traffic counters are published once per reactor loop, so the last PONG
    // can be visible to this side before the server has published its count.
    unsigned long long framesSent = 0;
    unsigned long long bytesSent  = 0;
    for (int i = 0; i < 100; ++i) {
        framesSent = server.stats().framesSent.load();
        bytesSent  = server.stats().bytesSent.load();
        if (framesSent == static_cast<unsigned long long>(kPings) &&
            bytesSent == static_cast<unsigned long long>(kPings) *
                             industrial::FRAME_HEADER_SIZE) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::printf("      PONG %d of %d (malformed %d), framesSent %llu bytesSent %llu\n",
                got, kPings, malformed, framesSent, bytesSent);

    harness::expect(got == kPings, "every PING got a well-formed PONG");
    harness::expect(framesSent == static_cast<unsigned long long>(kPings),
                    "the PONGs are counted as frames");
    harness::expect(bytesSent == static_cast<unsigned long long>(kPings) *
                                    industrial::FRAME_HEADER_SIZE,
                    "bytesSent is exactly header-size per PONG, so the counters "
                    "reconcile");
}

// The PING is sent in flight with the close: the close has been asked for, has
// not finished, and the peer is still talking. That is the case the branch's
// third arm exists for.
void caseWhileClosing() {
    std::printf("while closing: five PINGs sent as the close goes out\n");
    std::atomic<uint64_t> id{0};
    harness::Fixture server(
        harness::quietConfig(),
        [&id](uint64_t cid, const uint8_t*, size_t) { id.store(cid); });
    if (!server.started()) {
        harness::expect(false, "start() ok");
        return;
    }

    harness::Peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    (void)peer.send(FrameType::DATA, "hi", 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // closeConnection() only asks: the close reaches the connection through the
    // reactor's queue, so a PING already in flight may legitimately be answered
    // before it takes effect. Asserting on what happens from the moment of the
    // request would be asserting on that race, and it does go both ways (5/0,
    // 3/2 and 4/1 across three runs).
    //
    // So: PING until one is counted as dropped, which is the observable point at
    // which the close is in effect, and assert about everything after that.
    server.server().closeConnection(id.load());
    unsigned long long dropped = 0;
    for (int i = 0; i < 20 && dropped == 0; ++i) {
        if (!peer.send(FrameType::PING, nullptr, 0)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        dropped = server.stats().framesDroppedOnClose.load();
    }
    // Give the last pre-close PONG a loop to be published before the baseline.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const unsigned long long sentAfterCloseBegan = server.stats().framesSent.load();

    for (int i = 0; i < 5; ++i) {
        if (!peer.send(FrameType::PING, nullptr, 0)) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const unsigned long long droppedNow = server.stats().framesDroppedOnClose.load();
    const unsigned long long sentNow    = server.stats().framesSent.load();
    const unsigned long long aborted    = server.stats().aborted.load();
    std::printf("      once closing: PINGs dropped +%llu, framesSent %llu -> %llu, "
                "aborted %llu\n",
                droppedNow - dropped, sentAfterCloseBegan, sentNow, aborted);

    harness::expect(dropped >= 1, "a PING reaches a connection that is closing");
    harness::expect(droppedNow > dropped,
                    "PINGs arriving while closing are counted as dropped");
    harness::expect(sentNow == sentAfterCloseBegan,
                    "and none of them is answered: no frame leaves after the close "
                    "began");
    harness::expect(aborted == 0, "the close stayed graceful");
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    caseHealthy();
    caseWhileClosing();
    std::printf("\n");
    return harness::finish();
}
