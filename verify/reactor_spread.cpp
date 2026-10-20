// verify/reactor_spread.cpp
//
// What this covers that nothing else did: pickReactor() hands accepted
// connections out round-robin, and until this file existed no case in the tree
// asked which reactor owns a connection. Every other suite either pins
// numReactors = 1 or, like close_semantics, uses two and exercises both -- but
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

using industrial::FrameType;
using industrial::ServerStats;

namespace {

bool waitFor(int ms, const std::function<bool()>& done) {
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

    const int kConnections = 8;
    int reactors = 4;
    if (const char* r = std::getenv("RS_REACTORS")) reactors = std::atoi(r);
    if (reactors < 1) reactors = 1;

    std::printf("== %d connections over %d reactor(s) ==\n", kConnections, reactors);

    industrial::MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;        // let the kernel choose; port() reports it
    cfg.numReactors   = reactors;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;    // nothing here may end by a timer
    cfg.closeLingerMs = 300;

    std::mutex ownersMutex;
    std::map<uint64_t, std::thread::id> owners;   // connection -> its reactor
    std::atomic<int> framesHandled{0};
    std::atomic<int> closeCallbacks{0};

    harness::Fixture server(
        cfg,
        [&](uint64_t id, const uint8_t*, size_t) {
            std::lock_guard<std::mutex> lock(ownersMutex);
            owners[id] = std::this_thread::get_id();
            framesHandled.fetch_add(1);
        },
        [&](uint64_t, const std::string&, const char*) { closeCallbacks.fetch_add(1); });

    if (!server.started()) {
        harness::expect(false, "start() ok");
        return harness::finish();
    }

    // Port 0 means the kernel picked, so this is the only place the number
    // exists. A server that reports 0 here has connections going nowhere.
    const uint16_t port = server.port();
    harness::expect(port != 0, "port() reports the port the kernel chose");

    std::vector<harness::Peer> peers(kConnections);
    int connected = 0;
    for (auto& p : peers) {
        if (!p.connect(port)) break;
        ++connected;
    }
    harness::expect(connected == kConnections, "all clients connected");
    if (connected != kConnections) return harness::finish();

    // One frame each: it registers each connection with a reactor and makes
    // that reactor run the handler, which is where the owner becomes visible.
    int sent = 0;
    for (auto& p : peers) {
        if (p.send(FrameType::DATA, "hi", 2)) ++sent;
    }
    harness::expect(sent == kConnections, "one frame sent on every connection");

    const int expectedOwners = std::min(reactors, kConnections);
    waitFor(5000, [&] { return framesHandled.load() == kConnections; });
    harness::expect(framesHandled.load() == kConnections,
                    "every frame reached a handler");

    std::map<std::thread::id, int> perThread;
    {
        std::lock_guard<std::mutex> lock(ownersMutex);
        for (const auto& kv : owners) ++perThread[kv.second];
    }
    std::printf("      %zu connection(s) seen from %zu thread(s), expected %d:\n",
                owners.size(), perThread.size(), expectedOwners);
    for (const auto& kv : perThread) {
        std::printf("        thread %08zx handled %d\n",
                    std::hash<std::thread::id>{}(kv.first) & 0xffffffffu,
                    kv.second);
    }
    // The distribution itself, not the balance: round-robin over an atomic
    // counter owes every connection the next reactor in turn, but the count
    // per thread is not asserted -- a rejected connection advances the counter
    // without consuming a slot, and a test that forbids that would be
    // forbidding a documented behaviour.
    harness::expect(static_cast<int>(perThread.size()) == expectedOwners,
                    "connections are spread over min(reactors, connections) threads");

    // Close every connection from the main thread, which owns none of them.
    // Each of these is therefore a cross-reactor submission, one per
    // connection, and a queue that silently drops them shows up as a
    // connection that never closes rather than as a crash.
    const std::vector<uint64_t> ids = server.server().connectionIds();
    harness::expect(static_cast<int>(ids.size()) == kConnections,
                    "the registry holds every connection");
    int accepted = 0;
    for (uint64_t id : ids) {
        if (server.server().closeConnection(id)) ++accepted;
    }
    harness::expect(accepted == kConnections, "every close request was accepted");

    // Our end has to go too, or each close waits out closeLingerMs.
    for (auto& p : peers) p.close();

    harness::expect(waitFor(5000, [&] { return server.server().connectionCount() == 0; }),
                    "every connection closed");
    waitFor(2000, [&] { return closeCallbacks.load() == kConnections; });

    const ServerStats& s = server.stats();
    harness::expect(s.accepted.load() == static_cast<uint64_t>(kConnections),
                    "accepted counts each connection once");
    harness::expect(s.rejected.load() == 0, "nothing was rejected");
    harness::expect(s.reactorFailures.load() == 0, "no reactor failed");
    harness::expect(s.closeRequestsDropped.load() == 0, "no close request was dropped");
    harness::expect(s.queueDrops.load() == 0, "no queued task was dropped");
    harness::expect(closeCallbacks.load() == kConnections,
                    "every close ran its callback exactly once");

    server.stop();
    harness::expect(server.port() == 0, "a stopped server reports no port");
    harness::expect(server.server().connectionCount() == 0,
                    "a stopped server reports no connections");

    std::printf("\n");
    return harness::finish();
}
