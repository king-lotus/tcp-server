// verify/backpressure.cpp -- what one round of replies costs a single reactor.
//
// The task queue cap used to be a flat 8192, per reactor, and nothing said so.
// A broadcast is the shape that notices: every connection the server owns gets a
// reply queued on whichever reactor holds it, so the number that matters is not
// "how many connections does the server have" but "how many can land on one
// worker". With the default maxConnections of 10000 and one reactor, the first
// ~8192 subscribers got their message and the rest were counted as queueDrops --
// which reads like peers failing to drain, not like a constant.
//
// So this case fills one reactor's queue past the old cap and past nothing else:
//
//   - wbufCapacity and maxPendingBytes are both raised far above what the test
//     sends, so neither can be the thing that refuses a frame. A test whose
//     subject is one limit has to make the other limits unreachable, or it will
//     report whichever one tripped first and call it an answer.
//   - The worker is then held inside the open callback. A queue only fills if
//     nothing is draining it, and that is exactly what "the worker is busy doing
//     something else" means; letting it drain would turn the assertion into a
//     race between the producer and the consumer.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/backpressure.cpp -o backpressure
//
// Exit code 0 = the fan-out fit.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::MultiReactorServer;
using industrial::SendResult;

namespace {

bool waitFor(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    // Past the flat floor the cap used to be, comfortably inside what a single
    // reactor serving all of maxConnections should be able to queue.
    const int kFanOut = 9000;

    std::printf("A. one reactor has to be able to queue one round of replies\n");

    // ---- A: one round of replies has to fit in one reactor's queue ----------
    {
    MultiReactorServer::Config cfg;
    cfg.bindAddr = "127.0.0.1";
    // One worker, so every submission lands on the same queue and nothing gets
    // averaged away across reactors.
    cfg.numReactors = 1;
    // See the header: both raised until neither can be the limit being measured.
    cfg.maxPendingBytes = 8u << 20;
    cfg.wbufCapacity    = 8u << 20;

    std::atomic<bool>   release{false};
    std::atomic<uint64_t> connId{0};
    std::atomic<bool>    reachedCb{false};

    // Blocking the worker with a mutex plus a condition variable is the obvious
    // spelling and it is what this used to do. Not here: this machine's
    // ThreadSanitizer mis-models std::condition_variable::wait_for and reports
    // "double lock of a mutex" followed by a race on the flag guarded by that
    // mutex, with no server in the process at all -- see
    // verify/probe_tsan_condvar.sh. Suppressing it would leave the codebase
    // arguing with a sanitizer, so wait on an atomic with a deadline instead.
    // Same property being tested, nothing left to mis-model.
    harness::Fixture server(
        cfg, {}, {},
        [&](uint64_t id, const std::string&, bool up) {
            if (!up) return;
            connId.store(id, std::memory_order_release);
            reachedCb.store(true, std::memory_order_release);
            for (int i = 0; i < 20000 &&
                            !release.load(std::memory_order_acquire); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

    harness::expect(server.started(), "start() ok");

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(server.port())), "client connected");
    harness::expect(waitFor(5000, [&] { return reachedCb.load(std::memory_order_acquire); }),
           "the worker reached the open callback");

    const uint64_t before = server.stats().queueDrops.load();
    int accepted = 0;
    int refused  = 0;
    for (int i = 0; i < kFanOut; ++i) {
        const uint64_t id = connId.load(std::memory_order_acquire);
        const SendResult r = server.server().sendTo(id, "\x01", 1);
        if (r == SendResult::Ok) ++accepted;
        else                     ++refused;
    }

    release.store(true, std::memory_order_release);

    harness::expect(accepted == kFanOut, "every frame of a full fan-out was queued");
    harness::expect(server.stats().queueDrops.load() == before,
           "nothing was counted as a queue drop");
    std::printf("      queued %d of %d (%d refused), queueDrops=%llu\n",
                accepted, kFanOut, refused,
                (unsigned long long)(server.stats().queueDrops.load() - before));

    server.stop();
    peer.close();
    }

    // ---- B: a close the queue refuses must say so ---------------------------
    //
    // Same construction as A (one held worker, everything else raised out of
    // reach) but asked to close instead of send: with the queue full, enqueue
    // refuses, and until that is counted nothing anywhere records that this
    // connection was ever asked to go away -- it just waits for idle timeout.
    {
        std::printf("B. a close the queue refuses is countable\n");

        MultiReactorServer::Config bcfg;
        bcfg.bindAddr = "127.0.0.1";
        bcfg.numReactors = 1;
        // Same reasoning as case A: neither per-connection limit may be the
        // thing refusing anything here, so both are out of reach.
        bcfg.maxPendingBytes = 8u << 20;
        bcfg.wbufCapacity    = 8u << 20;

        std::atomic<bool>     release{false};
        std::atomic<uint64_t> connId{0};
        std::atomic<bool>     reachedCb{false};

        harness::Fixture server(
            bcfg, {}, {},
            [&](uint64_t id, const std::string&, bool up) {
                if (!up) return;
                connId.store(id, std::memory_order_release);
                reachedCb.store(true, std::memory_order_release);
                for (int i = 0; i < 20000 &&
                                !release.load(std::memory_order_acquire); ++i) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });

        harness::expect(server.started(), "start() ok");
        harness::Peer peer;
        harness::expect(static_cast<bool>(peer.connect(server.port())), "client connected");
        harness::expect(waitFor(5000, [&] { return reachedCb.load(std::memory_order_acquire); }),
               "the worker reached the open callback");

        // Fill past the queue cap: with one reactor and maxConnections at its
        // default the cap is 4 * maxConnections, so nothing below 40000 tasks
        // can make enqueue refuse. Nothing is draining any of it.
        int refused = 0;
        for (int i = 0; i < 45000; ++i) {
            const uint64_t id = connId.load(std::memory_order_acquire);
            if (server.server().sendTo(id, "\x01", 1) != SendResult::Ok) ++refused;
        }
        std::printf("      queue refused %d of 45000 sends\n", refused);
        harness::expect(refused > 0, "the queue cap actually refused frames");

        const uint64_t before = server.stats().closeRequestsDropped.load();
        // The returned bool is the same failure the caller can see; the counter
        // is the one operations can see. Neither used to exist.
        const bool queued = server.server().closeConnection(
            connId.load(std::memory_order_acquire));
        std::printf("      closeRequestsDropped +%llu, returned %s\n",
                    (unsigned long long)(server.stats().closeRequestsDropped.load()
                                         - before),
                    queued ? "true" : "false");
        harness::expect(!queued, "closeConnection() reports that it did not get through");
        harness::expect(server.stats().closeRequestsDropped.load() > before,
               "a close the queue refused is counted, not swallowed");

        release.store(true, std::memory_order_release);
        server.stop();
        peer.close();
    }

    return harness::finish();
}
