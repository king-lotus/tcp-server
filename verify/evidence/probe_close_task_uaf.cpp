// verify/evidence/probe_close_task_uaf.cpp
//
// R9-1: close_task / drain_input_task released their shared_ptr before calling
// drain_input_then_close(), so the connection was held alive only by conns_ and
// by the notification entry teardown_closed() is about to queue. An allocation
// failure while queueing that notification destroys the entry -- and with it
// the last reference -- while drain_input_then_close() is still testing
// c->is_closed. That is a heap-use-after-free, and it is the same bad_alloc
// window R5-7 closed for the notification itself.
//
// The scenario is arranged so the teardown happens *inside* the drain:
//
//   1. the client sends 300 complete DATA frames followed by one frame whose
//      magic is wrong, all in one burst;
//   2. the message handler calls close_connection() on frame 100 and then arms
//      the allocator wrapper;
//   3. the reactor's per-call parse budget is 256 frames, so the bad frame is
//      still in rbuf when the queued close_task runs;
//   4. handle_task(close_task) calls drain_input_then_close(), whose
//      handle_read() parses the bad frame and reaches teardown_closed();
//   5. __wrap__Znwm fails the pending_closes_ push_back. entry holds the last
//      reference, so its destructor frees the connection;
//   6. drain_input_then_close() reads c->is_closed -> use-after-free.
//
// The peer string is "127.0.0.1:<port>" (15 characters), which fits libstdc++'s
// small-string buffer, so the failing allocation is the vector growth in
// pending_closes_.push_back(), not the std::string copy.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -g -O1 -fsanitize=address
//       -fno-omit-frame-pointer -Iinclude -Wl,--wrap=_Znwm
//       verify/evidence/probe_close_task_uaf.cpp -o /tmp/probe_close_task_uaf
//   /tmp/probe_close_task_uaf
//
// Without ASan the freed memory often still reads as is_closed == true, so the
// binary is meant to be built with ASan: the pre-fix run is the sanitizer
// report, the post-fix run is exit 0 with close_callbacks_dropped == 1.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

// Read from inside the linker wrapper, which must have C linkage. Atomics keep
// the arm/disarm visible regardless of which thread reaches operator new.
std::atomic<int> g_arm{0};
std::atomic<int> g_new_failures{0};

extern "C" {
void* __real__Znwm(size_t);
void* __wrap__Znwm(size_t n) {
    if (g_arm.exchange(0)) {
        g_new_failures.fetch_add(1);
        throw std::bad_alloc();
    }
    return __real__Znwm(n);
}
}

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

int connect_to(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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

bool send_all(int fd, const void* p, size_t n) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    size_t off = 0;
    while (off < n) {
        ssize_t k = ::send(fd, b + off, n - off, 0);
        if (k <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

bool wait_for(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("R9-1: bad_alloc while the close notification is queued, "
                "teardown reached from inside drain_input_then_close().\n");

    multi_reactor_server::config cfg;
    cfg.bind_addr       = "127.0.0.1";
    cfg.port            = 0;
    cfg.num_reactors    = 1;
    cfg.tick_ms         = 20;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 5000;

    multi_reactor_server server(cfg);
    std::atomic<uint64_t> last_id{0};
    std::atomic<int>      frames{0};
    std::atomic<int>      close_calls{0};
    server.set_message_handler(
        [&](uint64_t cid, const uint8_t*, size_t) {
            last_id.store(cid);
            const int n = frames.fetch_add(1, std::memory_order_relaxed) + 1;
            if (n == 100) {
                // Queue the close while the parse budget still has frames to
                // spend, then arm the injection for the teardown that the
                // bad frame will reach from inside the drain.
                server.close_connection(cid);
                g_arm.store(1);
            }
        });
    server.set_close_handler(
        [&](uint64_t, const std::string&, const char*) {
            close_calls.fetch_add(1, std::memory_order_relaxed);
        });

    if (!server.start()) {
        std::printf("[FAIL] start()\n");
        return 1;
    }

    int fd = connect_to(server.port());
    if (fd < 0) {
        std::printf("[FAIL] connect()\n");
        server.stop();
        return 1;
    }

    // 300 well-formed frames then one with a bad magic, in one send so the
    // client cannot accidentally let the reactor run between them.
    std::vector<uint8_t> burst;
    burst.reserve(301 * industrial::FRAME_HEADER_SIZE);
    uint8_t one[industrial::FRAME_HEADER_SIZE];
    industrial::encode_frame_into(one, frame_type::DATA, nullptr, 0);
    for (int i = 0; i < 300; ++i)
        burst.insert(burst.end(), one, one + sizeof(one));
    uint8_t bad[industrial::FRAME_HEADER_SIZE];
    std::memcpy(bad, one, sizeof(bad));
    bad[0] = 0x00;
    bad[1] = 0x00;
    burst.insert(burst.end(), bad, bad + sizeof(bad));
    if (!send_all(fd, burst.data(), burst.size())) {
        std::printf("[FAIL] send()\n");
        ::close(fd);
        server.stop();
        return 1;
    }

    // The dropped notification is the proof that the injected allocation
    // failure reached teardown_closed()'s queueing block.
    const bool reached = wait_for(5000, [&] {
        return server.stats().close_callbacks_dropped.load() >= 1 ||
               !server.is_running();
    });
    const uint64_t dropped =
        server.stats().close_callbacks_dropped.load(std::memory_order_relaxed);
    const uint64_t closed = server.stats().closed.load(
        std::memory_order_relaxed);
    std::printf("frames dispatched %d, injected failures %d, closed %llu, "
                "close_callbacks_dropped %llu, close handler calls %d, "
                "running %d\n",
                frames.load(), g_new_failures.load(),
                (unsigned long long)closed, (unsigned long long)dropped,
                close_calls.load(), server.is_running() ? 1 : 0);

    int failures = 0;
    if (!reached || dropped < 1) {
        std::printf("[FAIL] the injected failure did not reach the close "
                    "notification queue\n");
        ++failures;
    }
    if (!server.is_running()) {
        std::printf("[FAIL] the server stopped\n");
        ++failures;
    }
    if (failures == 0) {
        std::printf("[PASS] no use-after-free; the lost notification was "
                    "counted\n");
    }

    ::close(fd);
    server.stop();
    return failures == 0 ? 0 : 1;
}
