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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::MultiReactorServer;

namespace {

int failures = 0;

void check(bool ok, const char* what, const char* detail) {
    std::printf("  [%s] %s%s%s\n", ok ? "PASS" : "FAIL", what,
                (detail && *detail) ? ": " : "",
                (detail && *detail) ? detail : "");
    if (!ok) ++failures;
}

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

int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
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
    check(cfg.wbufCapacity >= frameCeiling,
          "wbufCapacity can hold the largest legal frame", detail);

    // 128KB per connection is 1.28GB at the default maxConnections before any
    // per-connection struct, so a default above this is a memory regression.
    std::snprintf(detail, sizeof detail, "%zu bytes (budget %zu)",
                  cfg.wbufCapacity, static_cast<size_t>(128 * 1024));
    check(cfg.wbufCapacity <= 128 * 1024,
          "wbufCapacity is inside the per-connection budget", detail);

    std::snprintf(detail, sizeof detail, "maxPendingBytes=%zu wbufCapacity=%zu",
                  cfg.maxPendingBytes, cfg.wbufCapacity);
    check(cfg.maxPendingBytes <= cfg.wbufCapacity,
          "maxPendingBytes does not require a clamp at start()", detail);

    std::snprintf(detail, sizeof detail, "%zu bytes, one maximum frame is %zu",
                  cfg.rbufCapacity, frameCeiling);
    check(cfg.rbufCapacity >= frameCeiling,
          "rbufCapacity can hold the largest legal frame", detail);

    std::snprintf(detail, sizeof detail, "maxConnections=%d", cfg.maxConnections);
    check(cfg.maxConnections > 0, "maxConnections admits something", detail);

    std::printf("== measured cost (evidence, not a verdict) ==\n");

    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) {
        std::printf("  [FAIL] a default Config did not start\n");
        return 1;
    }

    const long before = rssKb();
    std::vector<int> fds;
    const size_t want = 200;
    for (size_t i = 0; i < want; ++i) {
        const int fd = connectTo(server.port());
        if (fd < 0) break;
        fds.push_back(fd);
    }
    for (int i = 0; i < 200 && server.connectionCount() < fds.size(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const long after = rssKb();

    if (!fds.empty() && before > 0 && after > 0) {
        std::printf("  [info] %.1f KB/connection over %zu connections\n",
                    static_cast<double>(after - before) /
                        static_cast<double>(fds.size()),
                    fds.size());
    } else {
        std::printf("  [info] RSS unreadable on this host; measurement skipped\n");
    }

    server.stop();
    for (int fd : fds) ::close(fd);

    if (failures == 0) {
        std::printf("ALL PASS (0 failures)\n");
    } else {
        std::printf("%d check(s) failed\n", failures);
    }
    return failures == 0 ? 0 : 1;
}
