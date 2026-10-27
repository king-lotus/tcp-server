// verify/reactor_spread.cpp
//
// What this covers that nothing else did: pick_reactor() hands accepted
// connections out round-robin, and until this file existed no case in the tree
// asked which reactor owns a connection. Every other suite either pins
// num_reactors = 1 or, like close_semantics, uses two and exercises both -- but
// "both workers behave" and "the connections are actually spread over them" are
// different claims, and only the first one had a test. An implementation that
// always returned reactors_[0] left all 21 configurations green.
//
// The owner needs no new API to observe. A reactor runs its handlers on its own
// thread, so the thread that calls the message handler is the owner. Eight
// connections over four reactors must therefore come back as exactly four
// distinct threads, and closing all eight from the main thread -- which owns
// none of them -- walks the cross-reactor submission path once per connection.
//
// What is asserted:
//   1. Accepted connections are spread over min(reactors, connections) threads.
//   2. Closing every connection from a thread that owns none of them is
//      accepted, completes, and is counted -- with no reactor failure, no
//      dropped close request and no dropped queued task.
//   3. A stopped server reports no port and no connections.
//
// RS_REACTORS overrides the count so the assertion can be shown to follow the
// configuration rather than a constant: the runner takes it down to two.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread -Iinclude verify/reactor_spread.cpp

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"

using industrial::frame_type;
using industrial::server_stats;

namespace {

bool wait_for(int ms, const std::function<bool()>& done) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return done();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    const int k_connections = 8;
    int reactors = 4;
    if (const char* r = std::getenv("RS_REACTORS")) reactors = std::atoi(r);
    if (reactors < 1) reactors = 1;

    std::printf("== %d connections over %d reactor(s) ==\n", k_connections, reactors);

    industrial::multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port          = 0;        // let the kernel choose; port() reports it
    cfg.num_reactors   = reactors;
    cfg.tick_ms        = 50;
    cfg.idle_timeout_ms = 60000;    // nothing here may end by a timer
    cfg.close_linger_ms = 300;

    std::mutex owners_mutex;
    std::map<uint64_t, std::thread::id> owners;   // connection -> its reactor
    std::atomic<int> frames_handled{0};
    std::atomic<int> close_callbacks{0};

    harness::fixture server(
        cfg,
        [&](uint64_t id, const uint8_t*, size_t) {
            std::lock_guard<std::mutex> lock(owners_mutex);
            owners[id] = std::this_thread::get_id();
            frames_handled.fetch_add(1);
        },
        [&](uint64_t, const std::string&, const char*) { close_callbacks.fetch_add(1); });

    if (!server.started()) {
        harness::expect(false, "start() ok");
        return harness::finish();
    }

    // Port 0 means the kernel picked, so this is the only place the number
    // exists. A server that reports 0 here has connections going nowhere.
    const uint16_t port = server.port();
    harness::expect(port != 0, "port() reports the port the kernel chose");

    std::vector<harness::peer> peers(k_connections);
    int connected = 0;
    for (auto& p : peers) {
        if (!p.connect(port)) break;
        ++connected;
    }
    harness::expect(connected == k_connections, "all clients connected");
    if (connected != k_connections) return harness::finish();

    // One frame each: it registers each connection with a reactor and makes
    // that reactor run the handler, which is where the owner becomes visible.
    int sent = 0;
    for (auto& p : peers) {
        if (p.send(frame_type::DATA, "hi", 2)) ++sent;
    }
    harness::expect(sent == k_connections, "one frame sent on every connection");

    const int expected_owners = std::min(reactors, k_connections);
    wait_for(5000, [&] { return frames_handled.load() == k_connections; });
    harness::expect(frames_handled.load() == k_connections,
                    "every frame reached a handler");

    std::map<std::thread::id, int> per_thread;
    {
        std::lock_guard<std::mutex> lock(owners_mutex);
        for (const auto& kv : owners) ++per_thread[kv.second];
    }
    std::printf("      %zu connection(s) seen from %zu thread(s), expected %d:\n",
                owners.size(), per_thread.size(), expected_owners);
    for (const auto& kv : per_thread) {
        std::printf("        thread %08zx handled %d\n",
                    std::hash<std::thread::id>{}(kv.first) & 0xffffffffu,
                    kv.second);
    }
    // The distribution itself, not the balance: round-robin over an atomic
    // counter owes every connection the next reactor in turn, but the count
    // per thread is not asserted -- a rejected connection advances the counter
    // without consuming a slot, and a test that forbids that would be
    // forbidding a documented behaviour.
    harness::expect(static_cast<int>(per_thread.size()) == expected_owners,
                    "connections are spread over min(reactors, connections) threads");

    // Close every connection from the main thread, which owns none of them.
    // Each of these is therefore a cross-reactor submission, one per
    // connection, and a queue that silently drops them shows up as a
    // connection that never closes rather than as a crash.
    const std::vector<uint64_t> ids = server.server().connection_ids();
    harness::expect(static_cast<int>(ids.size()) == k_connections,
                    "the registry holds every connection");
    int accepted = 0;
    for (uint64_t id : ids) {
        if (server.server().close_connection(id)) ++accepted;
    }
    harness::expect(accepted == k_connections, "every close request was accepted");

    // Our end has to go too, or each close waits out close_linger_ms.
    for (auto& p : peers) p.close();

    harness::expect(wait_for(5000, [&] { return server.server().connection_count() == 0; }),
                    "every connection closed");
    wait_for(2000, [&] { return close_callbacks.load() == k_connections; });

    const server_stats& s = server.stats();
    harness::expect(s.accepted.load() == static_cast<uint64_t>(k_connections),
                    "accepted counts each connection once");
    harness::expect(s.rejected.load() == 0, "nothing was rejected");
    harness::expect(s.reactor_failures.load() == 0, "no reactor failed");
    harness::expect(s.close_requests_dropped.load() == 0, "no close request was dropped");
    harness::expect(s.queue_drops.load() == 0, "no queued task was dropped");
    harness::expect(close_callbacks.load() == k_connections,
                    "every close ran its callback exactly once");

    server.stop();
    harness::expect(server.port() == 0, "a stopped server reports no port");
    harness::expect(server.server().connection_count() == 0,
                    "a stopped server reports no connections");

    std::printf("\n");
    return harness::finish();
}
