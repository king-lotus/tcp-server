// verify/latency_histograms.cpp -- opt-in reactor latency histograms.
//
// Build:
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread
//       -Iinclude verify/latency_histograms.cpp -o latency_histograms
//
// The three histograms are empty in a default build. Case A runs the same
// workload with the flag off; B and C enable it and assert the samples the
// flag is supposed to buy. fixture::stop() joins the reactor before any
// histogram is read, so the per-bucket counts are frozen at that point.

#include "harness.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace {

bool wait_for(int timeout_ms, const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

uint64_t samples(const industrial::latency_histogram& h) {
    return h.samples();
}

uint64_t total(const industrial::latency_histogram& h) {
    uint64_t n = 0;
    for (size_t i = 0; i < industrial::latency_histogram::kBuckets; ++i) {
        n += h.bucket(i);
    }
    return n;
}

bool empty(const industrial::latency_histogram& h) {
    return samples(h) == 0 && total(h) == 0;
}

void case_flag_off() {
    std::printf("A. histograms stay empty without the opt-in flag\n");
    auto cfg = harness::quiet_config();
    cfg.num_reactors = 1;
    std::atomic<int> calls{0};
    harness::fixture f(cfg, [&](uint64_t, const uint8_t*, size_t) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        calls.fetch_add(1, std::memory_order_relaxed);
    });
    harness::expect(f.started(), "server started");

    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");
    const uint8_t byte = 0x51;
    for (int i = 0; i < 8; ++i) {
        harness::expect(!!p.send(industrial::frame_type::DATA, &byte, 1),
                        "DATA frame sent");
    }
    harness::expect(wait_for(2000, [&] {
        return calls.load(std::memory_order_relaxed) == 8;
    }), "all eight frames reached the handler");
    f.stop();

    const auto& st = f.stats();
    harness::expect(empty(st.handler_duration_us), "handler histogram empty");
    harness::expect(empty(st.queue_wait_us), "queue histogram empty");
    harness::expect(empty(st.loop_lag_us), "loop histogram empty");
}

void case_handler_duration() {
    std::printf("B. a sleeping handler is measured in the >=1024us bucket\n");
    auto cfg = harness::quiet_config();
    cfg.num_reactors = 1;
    cfg.latency_histograms = true;
    std::atomic<int> calls{0};
    harness::fixture f(cfg, [&](uint64_t, const uint8_t*, size_t) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        calls.fetch_add(1, std::memory_order_relaxed);
    });
    harness::expect(f.started(), "server started");

    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");
    const uint8_t byte = 0x51;
    harness::expect(!!p.send(industrial::frame_type::DATA, &byte, 1),
                    "DATA frame sent");
    harness::expect(wait_for(2000, [&] {
        return calls.load(std::memory_order_relaxed) == 1;
    }), "the handler ran once");
    f.stop();

    const auto& st = f.stats();
    harness::expect(samples(st.handler_duration_us) == 1,
                    "one handler duration recorded");
    const size_t overflow = industrial::latency_histogram::kBuckets - 1;
    harness::expect(st.handler_duration_us.bucket(overflow) == 1,
                    "the 2ms handler landed in the >=1024us bucket");
    harness::expect(total(st.handler_duration_us) == 1,
                    "handler buckets add up to the sample count");
}

void case_throwing_handler() {
    std::printf("C. a throwing handler is still sampled\n");
    auto cfg = harness::quiet_config();
    cfg.num_reactors = 1;
    cfg.latency_histograms = true;
    std::atomic<int> calls{0};
    harness::fixture f(cfg, [&](uint64_t, const uint8_t*, size_t) {
        calls.fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("handler failure");
    });
    harness::expect(f.started(), "server started");

    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");
    const uint8_t byte = 0x51;
    harness::expect(!!p.send(industrial::frame_type::DATA, &byte, 1),
                    "DATA frame sent");
    harness::expect(wait_for(2000, [&] {
        return calls.load(std::memory_order_relaxed) == 1;
    }), "the handler ran once");
    f.stop();

    const auto& st = f.stats();
    harness::expect(samples(st.handler_duration_us) == 1,
                    "the thrown call was still recorded");
}

// The only reactor is parked inside the handler, so tasks submitted from
// the test thread stay in the queue until it is released.
struct handler_gate {
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    uint64_t conn_id = 0;

    bool wait_entry(int timeout_ms) {
        std::unique_lock<std::mutex> lk(m);
        return cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                           [this] { return entered; });
    }

    void open() {
        {
            std::lock_guard<std::mutex> lk(m);
            release = true;
        }
        cv.notify_all();
    }
};

void case_queue_wait_and_loop_lag() {
    std::printf("D. queued tasks and timer expiries are sampled\n");
    auto cfg = harness::quiet_config();
    cfg.num_reactors = 1;
    cfg.latency_histograms = true;
    handler_gate gate;
    harness::fixture f(cfg, [&](uint64_t id, const uint8_t* p, size_t n) {
        if (n == 1 && p[0] == 'x') {
            std::unique_lock<std::mutex> lk(gate.m);
            gate.conn_id = id;
            gate.entered = true;
            gate.cv.notify_all();
            gate.cv.wait(lk, [&] { return gate.release; });
        }
    });
    harness::expect(f.started(), "server started");

    harness::peer p;
    harness::expect(!!p.connect(f.port()), "peer connected");
    const uint8_t trigger = 'x';
    harness::expect(!!p.send(industrial::frame_type::DATA, &trigger, 1),
                    "trigger frame sent");
    if (!gate.wait_entry(2000)) {
        gate.open();
        harness::expect(false, "handler reached the blocking gate");
        f.stop();
        return;
    }

    constexpr int kTasks = 32;
    const uint8_t payload = 'y';
    int accepted = 0;
    for (int i = 0; i < kTasks; ++i) {
        if (f.server().send_to(gate.conn_id, &payload, 1) ==
            industrial::send_result::ok) {
            ++accepted;
        }
    }
    // A lower bound on the wait: every task queued above must sit for at
    // least this long, so its sample cannot fall below the overflow bucket.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    gate.open();
    harness::expect(accepted == kTasks, "all queued sends were accepted");

    int received = 0;
    for (int i = 0; i < kTasks; ++i) {
        harness::frame out;
        if (!p.expect_frame(&out, 2000)) break;
        ++received;
    }
    harness::expect(received == kTasks, "all queued frames were delivered");

    // Wait for a few timer expiries rather than a fixed sleep: a fast machine
    // then cannot skip the assertion, and a slow one does not stall it.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(1000);
    while (samples(f.stats().loop_lag_us) < 3 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    f.stop();

    const auto& st = f.stats();
    harness::expect(samples(st.queue_wait_us) >=
                        static_cast<uint64_t>(kTasks),
                    "each queued task produced a queue-wait sample");
    const size_t overflow = industrial::latency_histogram::kBuckets - 1;
    harness::expect(st.queue_wait_us.bucket(overflow) >=
                        static_cast<uint64_t>(kTasks),
                    "the blocked tasks landed in the >=1024us bucket");
    harness::expect(samples(st.queue_wait_us) == total(st.queue_wait_us),
                    "queue buckets add up to the sample count");
    harness::expect(samples(st.loop_lag_us) > 0,
                    "timer expiries were sampled");
    harness::expect(samples(st.loop_lag_us) == total(st.loop_lag_us),
                    "loop buckets add up to the sample count");
}

}  // namespace

int main() {
    std::printf("== latency histograms ==\n");
    case_flag_off();
    case_handler_duration();
    case_throwing_handler();
    case_queue_wait_and_loop_lag();
    std::printf("== latency histograms end ==\n");
    return harness::finish();
}
