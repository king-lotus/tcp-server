// verify/config_budget.cpp -- what a default Config costs, and what it must
// still be able to do.
//
// The write buffer is allocated when a connection is accepted, so
// `wbufCapacity` is what decides whether the default `maxConnections = 10000`
// fits in RAM: 256KB bought 324.7KB/conn, one maximum frame buys 128.8KB.
// Nothing else in this directory notices either number, because every suite
// that cares sets the value explicitly -- backpressure raises both to 8MB. So
// this is the case that notices.
//
// The upper bound is a memory budget. The lower bound is not: sendTo() refuses
// a frame that does not fit in wbuf, so a buffer smaller than
// FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD makes the largest legal reply
// impossible to write at all -- and 64 * 1024, a number that reads as "64KB",
// is 8 bytes too small. That is how this case came to exist.
//
// Only the configuration half is fatal. RSS depends on the allocator and on the
// sanitizer the suite is built with, so a threshold there would fail on
// somebody else's glibc rather than on a regression: it is reported, not
// asserted.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <utility>
#include <vector>

#include "harness.h"

using industrial::MultiReactorServer;

namespace {

long rssKb() {
    FILE* f = std::fopen("/proc/self/status", "r");
    if (!f) return -1;
    char line[256];
    long kb = -1;
    while (std::fgets(line, sizeof(line), f)) {
        if (std::sscanf(line, "VmRSS: %ld kB", &kb) == 1) break;
    }
    std::fclose(f);
    return kb;
}

}  // namespace

int main() {
    MultiReactorServer::Config cfg;
    char detail[192];

    const size_t frameCeiling =
        industrial::FRAME_HEADER_SIZE + industrial::MAX_FRAME_PAYLOAD;

    std::printf("== what a default Config promises ==\n");

    std::snprintf(detail, sizeof detail, "%zu bytes, one maximum frame is %zu",
                  cfg.wbufCapacity, frameCeiling);
    harness::expect(cfg.wbufCapacity >= frameCeiling,
          "wbufCapacity can hold the largest legal frame", detail);

    // 128KB per connection is 1.28GB at the default maxConnections before any
    // per-connection struct, so a default above this is a memory regression.
    std::snprintf(detail, sizeof detail, "%zu bytes (budget %zu)",
                  cfg.wbufCapacity, static_cast<size_t>(128 * 1024));
    harness::expect(cfg.wbufCapacity <= 128 * 1024,
          "wbufCapacity is inside the per-connection budget", detail);

    std::snprintf(detail, sizeof detail, "maxPendingBytes=%zu wbufCapacity=%zu",
                  cfg.maxPendingBytes, cfg.wbufCapacity);
    harness::expect(cfg.maxPendingBytes <= cfg.wbufCapacity,
                    "maxPendingBytes does not require a clamp at start()", detail);

    std::snprintf(detail, sizeof detail, "%zu bytes, one maximum frame is %zu",
                  cfg.rbufCapacity, frameCeiling);
    harness::expect(cfg.rbufCapacity >= frameCeiling,
                    "rbufCapacity can hold the largest legal frame", detail);

    std::snprintf(detail, sizeof detail, "maxConnections=%d", cfg.maxConnections);
    harness::expect(cfg.maxConnections > 0, "maxConnections admits something", detail);

    std::printf("== measured cost (evidence, not a verdict) ==\n");

    harness::Fixture server(cfg);
    if (!server.started()) {
        harness::expect(false, "a default Config starts");
        return harness::finish();
    }

    const long before = rssKb();
    std::vector<harness::Peer> peers;
    const size_t want = 200;
    for (size_t i = 0; i < want; ++i) {
        harness::Peer p;
        if (!p.connect(server.port())) break;
        peers.push_back(std::move(p));
    }
    for (int i = 0; i < 200 && server.server().connectionCount() < peers.size(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const long after = rssKb();

    if (!peers.empty() && before > 0 && after > 0) {
        std::printf("  [info] %.1f KB/connection over %zu connections\n",
                    static_cast<double>(after - before) /
                        static_cast<double>(peers.size()),
                    peers.size());
    } else {
        std::printf("  [info] RSS unreadable on this host; measurement skipped\n");
    }

    return harness::finish();
}
