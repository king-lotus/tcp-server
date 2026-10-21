// verify/evidence/probe_stride_cache.cpp
//
// Does 8 + 65536 -- eight bytes past sixteen pages -- cost anything in cache?
//
// The question is not about one buffer's size, it is about the *stride*
// between consecutive buffers. Every connection allocates an rbuf of
// rbufCapacity bytes, so buffer k starts at k * stride. On x86-64 the L1 set
// index is bits [11:6], so a stride that is a multiple of 4096 puts every
// buffer on the same set; a stride that is not spreads them.
//
// 65536 is a multiple of 4096. 65544 is not. So the eight bytes look like they
// help -- but only if they survive to the cache. The allocator gets a vote:
// glibc rounds a request up to a 16-byte boundary and adds its own header, so
// the stride the cache sees is not the capacity asked for. Part A measures
// that. Part B measures whether the resulting stride costs anything. Part C
// measures the one thing the stride does introduce: adjacent buffers sharing a
// cache line.
//
//     g++ -std=c++17 -O2 -pthread verify/evidence/probe_stride_cache.cpp -o /tmp/psc
//     /tmp/psc

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

namespace {

constexpr size_t kBuffers = 512;    // 512 x 64 KiB = 32 MiB
constexpr int    kRounds  = 4000;

// ---- A. what stride does the allocator actually produce? --------------------
void reportStride(size_t cap) {
    std::vector<std::vector<uint8_t>> bufs;
    bufs.reserve(8);
    for (int i = 0; i < 8; ++i) bufs.emplace_back(cap, 0);
    size_t lo = SIZE_MAX, hi = 0;
    for (size_t i = 1; i < bufs.size(); ++i) {
        const auto a = reinterpret_cast<uintptr_t>(bufs[i - 1].data());
        const auto b = reinterpret_cast<uintptr_t>(bufs[i].data());
        if (b < a) {                       // large blocks come from mmap, in
            std::printf("cap=%7zu  stride=n/a  (adjacent blocks not ascending:"
                        " mmap, one mapping each)\n", cap);
            return;                        // descending order, so skip the row
        }
        const size_t d = static_cast<size_t>(b - a);
        if (d < lo) lo = d;
        if (d > hi) hi = d;
    }
    std::printf("cap=%7zu  stride=%6zu  stride%%4096=%3zu  stride%%64=%2zu\n",
                cap, lo, lo % 4096, lo % 64);
    (void)hi;
}

// ---- B. does touching one line per buffer cost anything? --------------------
double touchOneLineEach(size_t cap) {
    std::vector<std::vector<uint8_t>> bufs;
    bufs.reserve(kBuffers);
    for (size_t i = 0; i < kBuffers; ++i) bufs.emplace_back(cap, 0);
    const auto t0 = std::chrono::steady_clock::now();
    volatile uint64_t sink = 0;
    for (int r = 0; r < kRounds; ++r) {
        for (size_t i = 0; i < bufs.size(); ++i) {
            auto& b = bufs[i];
            // Read + write, so the line must be owned, not just shared: that
            // is what makes a set conflict visible as time.
            b[0] = static_cast<uint8_t>(b[0] + 1);
            sink += b[0];
        }
    }
    const auto t1 = std::chrono::steady_clock::now();
    (void)sink;
    return std::chrono::duration<double, std::nano>(t1 - t0).count() /
           (static_cast<double>(kRounds) * static_cast<double>(kBuffers));
}

// ---- C. two threads writing the same line vs two different lines -----------
// This is the one real cost a non-line-multiple stride introduces: buffer k's
// last line also holds buffer k+1's first bytes, so two connections owned by
// two reactors can write the same line.
double twoThreads(uint8_t* a, uint8_t* b, int iters) {
    std::atomic<bool> go{false};
    // volatile on purpose: without it the compiler folds the loop into a single
    // `*p += iters`, the threads stop touching memory, and the measurement
    // reports loop overhead. That mistake read as 0.39 ns/op, which is faster
    // than a contended line can possibly be -- the number was the tell.
    auto body = [&](uint8_t* p) {
        volatile uint8_t* v = p;
        while (!go.load(std::memory_order_acquire)) {}
        for (int i = 0; i < iters; ++i) {
            *v = static_cast<uint8_t>(*v + 1);
        }
    };
    std::thread t1(body, a), t2(body, b);
    const auto t0 = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    t1.join();
    t2.join();
    const auto t1e = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::nano>(t1e - t0).count() /
           static_cast<double>(iters);
}

}  // namespace

int main() {
    std::printf("page=4096  cache line=64  L1 set index = bits[11:6]\n\n");

    std::printf("A. the stride the allocator produces\n");
    reportStride(8 + 65536);
    reportStride(65536);
    reportStride(65600);
    reportStride(8 + 131072);
    reportStride(131072);

    std::printf("\nB. cost of touching one line per buffer, %zu buffers\n",
                kBuffers);
    for (size_t cap : {size_t(8 + 65536), size_t(65536), size_t(65600)}) {
        double best = 1e18;
        for (int rep = 0; rep < 3; ++rep) {
            const double v = touchOneLineEach(cap);
            if (v < best) best = v;
        }
        std::printf("   cap=%7zu  %6.2f ns/line (best of 3)\n", cap, best);
    }

    std::printf("\nC. two threads, same line vs different lines\n");
    {
        // One allocation, sliced at the stride measured in part A. Two separate
        // emplace_back calls are NOT reliably adjacent -- the heap leaves a gap
        // between them (measured: 65616 instead of 65552), which silently
        // turns this into a comparison of two unrelated lines. That is the
        // same class of mistake as the optimised-away loop: the test passes
        // and measures nothing.
        const size_t stride = 65552;            // what part A reports
        std::vector<uint8_t> block(2 * stride, 0);
        uint8_t* a = block.data();
        uint8_t* b = block.data() + stride;
        std::printf("   forced stride=%zu (stride%%64=%zu)\n", stride,
                    stride % 64);
        constexpr int kIters = 2000000;
        // a's last byte and b's first byte: same line when stride%64 != 0.
        std::printf("   same cache line       %8.2f ns/op\n",
                    twoThreads(a + stride - 1, b, kIters));
        // Each thread in its own region, one line apart.
        std::printf("   different cache lines %8.2f ns/op\n",
                    twoThreads(a, b + 1024, kIters));
    }
    return 0;
}
