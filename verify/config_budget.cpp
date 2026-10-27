// verify/config_budget.cpp -- what a default config costs, and what it must
// still be able to do.
//
// The write buffer is allocated when a connection is accepted, so
// `wbuf_capacity` is what decides whether the default `max_connections = 10000`
// fits in RAM: 256KB bought 324.7KB/conn, one maximum frame buys 128.8KB.
// Nothing else in this directory notices either number, because every suite
// that cares sets the value explicitly -- backpressure raises both to 8MB. So
// this is the case that notices.
//
// The upper bound is a memory budget. The lower bound is not: send_to() refuses
// a frame that does not fit in wbuf, so a buffer smaller than MAX_FRAME_SIZE
// makes the largest legal reply impossible to write at all -- and 64 * 1024, a
// number that reads as "64KB", is 8 bytes too small. That is how this case came
// to exist. A caller may still choose a smaller buffer, but then the largest
// reply is refused; the default must not.
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

using industrial::multi_reactor_server;

namespace {

long rss_kb() {
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
    multi_reactor_server::config cfg;
    char detail[192];

    const size_t frame_ceiling = industrial::MAX_FRAME_SIZE;

    std::printf("== what a default config promises ==\n");

    std::snprintf(detail, sizeof detail, "%zu bytes, one maximum frame is %zu",
                  cfg.wbuf_capacity, frame_ceiling);
    harness::expect(cfg.wbuf_capacity >= frame_ceiling,
          "wbuf_capacity can hold the largest legal frame", detail);

    // 128KB per connection is 1.28GB at the default max_connections before any
    // per-connection struct, so a default above this is a memory regression.
    std::snprintf(detail, sizeof detail, "%zu bytes (budget %zu)",
                  cfg.wbuf_capacity, static_cast<size_t>(128 * 1024));
    harness::expect(cfg.wbuf_capacity <= 128 * 1024,
          "wbuf_capacity is inside the per-connection budget", detail);

    std::snprintf(detail, sizeof detail, "max_pending_bytes=%zu wbuf_capacity=%zu",
                  cfg.max_pending_bytes, cfg.wbuf_capacity);
    harness::expect(cfg.max_pending_bytes <= cfg.wbuf_capacity,
                    "max_pending_bytes does not require a clamp at start()", detail);

    std::snprintf(detail, sizeof detail, "%zu bytes, one maximum frame is %zu",
                  cfg.rbuf_capacity, frame_ceiling);
    harness::expect(cfg.rbuf_capacity >= frame_ceiling,
                    "rbuf_capacity can hold the largest legal frame", detail);

    std::snprintf(detail, sizeof detail, "max_connections=%d", cfg.max_connections);
    harness::expect(cfg.max_connections > 0, "max_connections admits something", detail);

    // The in-place write API has one caller today, but a commit that trusted
    // its argument instead of the span it offered would mark bytes it never
    // wrote as readable -- stale data handed to parse_frames.
    industrial::ring_buffer ring(8);
    const uint8_t six[6] = {0, 1, 2, 3, 4, 5};
    ring.write(six, sizeof six);
    ring.consume(4);   // head_ = 4, size_ = 2, so only [6, 8) is contiguous
    std::snprintf(detail, sizeof detail, "write_span=%zu writable=%zu",
                  ring.write_span(), ring.writable());
    harness::expect(ring.write_span() == 2,
                    "write_span offers only the contiguous tail", detail);
    ring.commit(6);    // more than the ring offered
    std::snprintf(detail, sizeof detail, "readable=%zu", ring.readable());
    harness::expect(ring.readable() == 4,
                    "commit clamps to the offered span", detail);

    std::printf("== measured cost (evidence, not a verdict) ==\n");

    harness::fixture server(cfg);
    if (!server.started()) {
        harness::expect(false, "a default config starts");
        return harness::finish();
    }

    const long before = rss_kb();
    std::vector<harness::peer> peers;
    const size_t want = 200;
    for (size_t i = 0; i < want; ++i) {
        harness::peer p;
        if (!p.connect(server.port())) break;
        peers.push_back(std::move(p));
    }
    for (int i = 0; i < 200 && server.server().connection_count() < peers.size(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const long after = rss_kb();

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
