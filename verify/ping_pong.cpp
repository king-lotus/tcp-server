// verify/ping_pong.cpp -- the read path's most conditional branch, executed.
//
// parse_frames() answers PING itself: a reservation CAS loop against
// max_pending_bytes, a refusal that tears the connection down, and a "we are
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
//   2. The PONG is counted like any other frame. frames_sent and bytes_sent are
//      reconcilable only if every frame that leaves is counted, and a PONG is a
//      frame that leaves: bytes_sent must be 8 * PONG, 8 being the header.
//   3. A PING that lands after the close was asked for is dropped and counted
//      as frames_dropped_on_close. Answering it would put a frame out past the FIN,
//      which is the thing the whole close sequence exists to prevent.
//
// The client half now goes through verify/harness.h: this file used to carry
// its own pick_port/connect_to/send_all/frame/readFrame, like nine others.
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

using industrial::frame_type;

namespace {

void case_healthy() {
    std::printf("healthy: five PINGs on a connection that is not going anywhere\n");
    harness::fixture server(harness::quiet_config());
    if (!server.started()) {
        harness::expect(false, "start() ok");
        return;
    }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    const int k_pings = 5;
    int       got = 0;
    int       malformed = 0;
    for (int i = 0; i < k_pings; ++i) {
        if (!peer.send(frame_type::PING, nullptr, 0)) break;
        harness::frame f;
        if (!peer.expect_frame(&f, 500)) continue;
        if (f.type == frame_type::PONG && f.payload.empty()) ++got;
        else ++malformed;
    }

    // Traffic counters are published once per reactor loop, so the last PONG
    // can be visible to this side before the server has published its count.
    unsigned long long frames_sent = 0;
    unsigned long long bytes_sent  = 0;
    for (int i = 0; i < 100; ++i) {
        frames_sent = server.stats().frames_sent.load();
        bytes_sent  = server.stats().bytes_sent.load();
        if (frames_sent == static_cast<unsigned long long>(k_pings) &&
            bytes_sent == static_cast<unsigned long long>(k_pings) *
                             industrial::FRAME_HEADER_SIZE) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::printf("      PONG %d of %d (malformed %d), frames_sent %llu bytes_sent %llu\n",
                got, k_pings, malformed, frames_sent, bytes_sent);

    harness::expect(got == k_pings, "every PING got a well-formed PONG");
    harness::expect(frames_sent == static_cast<unsigned long long>(k_pings),
                    "the PONGs are counted as frames");
    harness::expect(bytes_sent == static_cast<unsigned long long>(k_pings) *
                                    industrial::FRAME_HEADER_SIZE,
                    "bytes_sent is exactly header-size per PONG, so the counters "
                    "reconcile");
}

// The PING is sent in flight with the close: the close has been asked for, has
// not finished, and the peer is still talking. That is the case the branch's
// third arm exists for.
void case_while_closing() {
    std::printf("while closing: five PINGs sent as the close goes out\n");
    std::atomic<uint64_t> id{0};
    harness::fixture server(
        harness::quiet_config(),
        [&id](uint64_t cid, const uint8_t*, size_t) { id.store(cid); });
    if (!server.started()) {
        harness::expect(false, "start() ok");
        return;
    }

    harness::peer peer;
    if (!peer.connect(server.port())) {
        harness::expect(false, "client connected");
        return;
    }

    (void)peer.send(frame_type::DATA, "hi", 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // close_connection() only asks: the close reaches the connection through the
    // reactor's queue, so a PING already in flight may legitimately be answered
    // before it takes effect. Asserting on what happens from the moment of the
    // request would be asserting on that race, and it does go both ways (5/0,
    // 3/2 and 4/1 across three runs).
    //
    // So: PING until one is counted as dropped, which is the observable point at
    // which the close is in effect, and assert about everything after that.
    server.server().close_connection(id.load());
    unsigned long long dropped = 0;
    for (int i = 0; i < 20 && dropped == 0; ++i) {
        if (!peer.send(frame_type::PING, nullptr, 0)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        dropped = server.stats().frames_dropped_on_close.load();
    }
    // Give the last pre-close PONG a loop to be published before the baseline.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const unsigned long long sent_after_close_began = server.stats().frames_sent.load();

    for (int i = 0; i < 5; ++i) {
        if (!peer.send(frame_type::PING, nullptr, 0)) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const unsigned long long dropped_now = server.stats().frames_dropped_on_close.load();
    const unsigned long long sent_now    = server.stats().frames_sent.load();
    const unsigned long long aborted    = server.stats().aborted.load();
    std::printf("      once closing: PINGs dropped +%llu, frames_sent %llu -> %llu, "
                "aborted %llu\n",
                dropped_now - dropped, sent_after_close_began, sent_now, aborted);

    harness::expect(dropped >= 1, "a PING reaches a connection that is closing");
    harness::expect(dropped_now > dropped,
                    "PINGs arriving while closing are counted as dropped");
    harness::expect(sent_now == sent_after_close_began,
                    "and none of them is answered: no frame leaves after the close "
                    "began");
    harness::expect(aborted == 0, "the close stayed graceful");
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    case_healthy();
    case_while_closing();
    std::printf("\n");
    return harness::finish();
}
