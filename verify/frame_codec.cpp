// verify/frame_codec.cpp -- the header's decisions, off the reactor.
//
// decode_header() is pure, so its whole input domain is eight bytes plus one
// policy flag. Three layers check it here:
//
//   1. a hand-written table for every status and for the boundary lengths;
//   2. an old byte-order copy -- the logic parse_frames used before the
//      extraction -- cross-checked over structured combinations and random
//      bytes, so the refactor's equivalence is a test rather than a claim;
//   3. LLVMFuzzerTestOneInput, which checks invariants on any input at all.
//      Built without libFuzzer (the way verify/run.sh builds it) this file
//      supplies its own deterministic driver, so the same entry point runs
//      in the ordinary suite and under clang's fuzzer.
//
// To run a real fuzzing session:
//   clang++ -std=c++17 -Iinclude -DTCP_FUZZING=1 -fsanitize=fuzzer,address verify/frame_codec.cpp -o decode_header_fuzz
//   ./decode_header_fuzz -dict=verify/fuzz/decode_header.dict -max_len=12 -max_total_time=60
//
// Linux only when built with the suite driver (harness.h); the fuzz target
// itself needs nothing but the header.

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "tcp-server-mt.h"

#ifndef TCP_FUZZING
#include "harness.h"
#endif

namespace {

using industrial::FRAME_HEADER_SIZE;
using industrial::FRAME_MAGIC;
using industrial::MAX_FRAME_PAYLOAD;
using industrial::decode_header;
using industrial::decoded_header;
using industrial::frame_type;
using industrial::header_status;

#ifndef TCP_FUZZING
std::array<uint8_t, FRAME_HEADER_SIZE> hdr(uint16_t magic, uint16_t type,
                                           uint32_t len) noexcept {
    return {{static_cast<uint8_t>(magic >> 8),
             static_cast<uint8_t>(magic),
             static_cast<uint8_t>(type >> 8),
             static_cast<uint8_t>(type),
             static_cast<uint8_t>(len >> 24),
             static_cast<uint8_t>(len >> 16),
             static_cast<uint8_t>(len >> 8),
             static_cast<uint8_t>(len)}};
}

const char* name_of(header_status s) noexcept {
    switch (s) {
        case header_status::need_more:       return "need_more";
        case header_status::ok:              return "ok";
        case header_status::bad_magic:       return "bad_magic";
        case header_status::unknown_type:    return "unknown_type";
        case header_status::frame_too_large: return "frame_too_large";
    }
    return "?";
}

struct row {
    const char*                          label;
    std::array<uint8_t, FRAME_HEADER_SIZE> bytes;
    size_t                               n;
    bool                                 allow;
    header_status                        status;
    frame_type                           type;
    uint32_t                             len;
};

void check_table() {
    const uint16_t t_data = static_cast<uint16_t>(frame_type::DATA);
    const uint16_t t_ping = static_cast<uint16_t>(frame_type::PING);
    const uint16_t t_pong = static_cast<uint16_t>(frame_type::PONG);
    const uint32_t max = MAX_FRAME_PAYLOAD;

    const row rows[] = {
        {"a short header is need_more, not an error",
         hdr(FRAME_MAGIC, t_data, 0), 3, false,
         header_status::need_more, frame_type::DATA, 0},
        {"an empty input needs nothing from the pointer",
         hdr(FRAME_MAGIC, t_data, 0), 0, false,
         header_status::need_more, frame_type::DATA, 0},
        {"a zero-length DATA frame is legal",
         hdr(FRAME_MAGIC, t_data, 0), FRAME_HEADER_SIZE, false,
         header_status::ok, frame_type::DATA, 0},
        {"the length is big-endian, not four unrelated bytes",
         hdr(FRAME_MAGIC, t_data, 0x00000102), FRAME_HEADER_SIZE, false,
         header_status::ok, frame_type::DATA, 258},
        {"PING keeps its type",
         hdr(FRAME_MAGIC, t_ping, 1), FRAME_HEADER_SIZE, false,
         header_status::ok, frame_type::PING, 1},
        {"PONG keeps its type",
         hdr(FRAME_MAGIC, t_pong, 0), FRAME_HEADER_SIZE, false,
         header_status::ok, frame_type::PONG, 0},
        {"the largest legal payload is accepted",
         hdr(FRAME_MAGIC, t_data, max), FRAME_HEADER_SIZE, false,
         header_status::ok, frame_type::DATA, max},
        {"one byte over the limit is rejected",
         hdr(FRAME_MAGIC, t_data, max + 1), FRAME_HEADER_SIZE, false,
         header_status::frame_too_large, frame_type::DATA, 0},
        {"a wrapped length is rejected, not truncated",
         hdr(FRAME_MAGIC, t_data, 0xFFFFFFFF), FRAME_HEADER_SIZE, false,
         header_status::frame_too_large, frame_type::DATA, 0},
        {"bad magic wins over a bad type and length",
         hdr(0x0000, 0xFFFF, 0xFFFFFFFF), FRAME_HEADER_SIZE, true,
         header_status::bad_magic, frame_type::DATA, 0},
        {"only the second magic byte differs",
         hdr(0xEB91, t_data, 0), FRAME_HEADER_SIZE, false,
         header_status::bad_magic, frame_type::DATA, 0},
        {"an unknown type is rejected before its length is read",
         hdr(FRAME_MAGIC, 0x0004, 0xFFFFFFFF), FRAME_HEADER_SIZE, false,
         header_status::unknown_type, frame_type::DATA, 0},
        {"an allowed unknown type is delivered as DATA",
         hdr(FRAME_MAGIC, 0x0004, 7), FRAME_HEADER_SIZE, true,
         header_status::ok, frame_type::DATA, 7},
        {"an allowed unknown type still has its length checked",
         hdr(FRAME_MAGIC, 0xFFFF, 0xFFFFFFFF), FRAME_HEADER_SIZE, true,
         header_status::frame_too_large, frame_type::DATA, 0},
        {"DATA itself is never an unknown type",
         hdr(FRAME_MAGIC, t_data, 0), FRAME_HEADER_SIZE, true,
         header_status::ok, frame_type::DATA, 0},
    };

    for (const row& r : rows) {
        const decoded_header got = decode_header(r.bytes.data(), r.n, r.allow);
        const bool passed = got.status == r.status && got.type == r.type &&
                            got.len == r.len;
        std::string detail;
        if (!passed) {
            detail = std::string("got status=") + name_of(got.status) +
                     ", type=" + std::to_string(static_cast<int>(got.type)) +
                     ", len=" + std::to_string(got.len) +
                     "; want status=" + name_of(r.status) +
                     ", type=" + std::to_string(static_cast<int>(r.type)) +
                     ", len=" + std::to_string(r.len);
        }
        harness::expect(passed, r.label, detail);
    }

    // The empty row above passes a real array, but the contract also allows
    // nullptr when nothing is readable. Exercise it directly: this is what a
    // fuzz target with an empty buffer does.
    const decoded_header empty = decode_header(nullptr, 0, false);
    harness::expect(empty.status == header_status::need_more && empty.len == 0,
                    "a null pointer with zero bytes is safe");
}
#endif  // TCP_FUZZING: the table and its helpers are not needed by libFuzzer

// ---------------------------------------------------------------------------
// The old byte-order copy. Kept only here, and deliberately not shared with
// the header: if both sides changed together, equivalence would prove nothing.
// ---------------------------------------------------------------------------
struct old_decision {
    int        error = 0;  // 0 none, 1 magic, 2 type, 3 length
    frame_type type   = frame_type::DATA;
    uint32_t   len    = 0;
};

old_decision old_decode(const uint8_t* p, bool allow_unknown) noexcept {
    old_decision d;
    const uint16_t magic = (static_cast<uint16_t>(p[0]) << 8) | p[1];
    if (magic != FRAME_MAGIC) { d.error = 1; return d; }
    const frame_type type = static_cast<frame_type>(
        (static_cast<uint16_t>(p[2]) << 8) | p[3]);
    if (type != frame_type::DATA && type != frame_type::PING &&
        type != frame_type::PONG && !allow_unknown) { d.error = 2; return d; }
    const uint32_t len = (static_cast<uint32_t>(p[4]) << 24)
                       | (static_cast<uint32_t>(p[5]) << 16)
                       | (static_cast<uint32_t>(p[6]) <<  8)
                       |  static_cast<uint32_t>(p[7]);
    if (len > MAX_FRAME_PAYLOAD) { d.error = 3; return d; }
    d.type = type;
    d.len  = len;
    return d;
}

// Returns nullptr when every invariant holds, otherwise the first reason.
// The fuzz entry point aborts on a non-null reason so libFuzzer keeps the
// offending input; the suite driver counts it instead.
const char* fuzz_check(const uint8_t* data, size_t size) noexcept {
    const bool allow = size > FRAME_HEADER_SIZE
                     ? (data[FRAME_HEADER_SIZE] & 1u) != 0 : false;
    const decoded_header h = decode_header(data, size, allow);

    if (h.status == header_status::need_more) {
        if (size >= FRAME_HEADER_SIZE)
            return "need_more with a complete header";
    } else {
        if (size < FRAME_HEADER_SIZE)
            return "a decision other than need_more on a short input";
        if (h.status == header_status::ok) {
            if (h.type != frame_type::DATA && h.type != frame_type::PING &&
                h.type != frame_type::PONG)
                return "ok with a type the dispatcher does not know";
            if (h.len > MAX_FRAME_PAYLOAD)
                return "ok with an over-long payload";
        } else if (h.type != frame_type::DATA || h.len != 0) {
            return "an error path exposed a type or length";
        }
    }

    // Pure means repeatable, and the header's meaning cannot depend on bytes
    // after the eighth. Both are properties a reactor-side copy would lack.
    const decoded_header again = decode_header(data, size, allow);
    if (again.status != h.status || again.type != h.type || again.len != h.len)
        return "decode_header is not deterministic";
    if (size > FRAME_HEADER_SIZE) {
        const decoded_header first_eight =
            decode_header(data, FRAME_HEADER_SIZE, allow);
        if (first_eight.status != h.status || first_eight.type != h.type ||
            first_eight.len != h.len)
            return "bytes after the header changed the decision";
    }

    if (size < FRAME_HEADER_SIZE) return nullptr;

    const old_decision want = old_decode(data, allow);
    const header_status want_status =
        want.error == 1 ? header_status::bad_magic
        : want.error == 2 ? header_status::unknown_type
        : want.error == 3 ? header_status::frame_too_large
                          : header_status::ok;
    if (h.status != want_status) return "disagrees with the old status";
    if (want.error == 0) {
        // An allowed unknown type used to travel as its raw value and fall
        // through to DATA delivery; decode_header normalises it to DATA.
        const bool same_type = want.type == h.type ||
            (allow && want.type != frame_type::DATA &&
             want.type != frame_type::PING && want.type != frame_type::PONG &&
             h.type == frame_type::DATA);
        if (!same_type) return "disagrees with the old type";
        if (want.len != h.len) return "disagrees with the old length";
    }
    return nullptr;
}

#ifndef TCP_FUZZING
uint64_t next_random(uint64_t& state) noexcept {
    // splitmix64: small, deterministic, and good enough to spread over the
    // header's byte positions without dragging <random> into every suite.
    state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

void check_random_inputs() {
    constexpr int kIterations = 200000;
    const uint16_t types[] = {
        static_cast<uint16_t>(frame_type::DATA),
        static_cast<uint16_t>(frame_type::PING),
        static_cast<uint16_t>(frame_type::PONG),
        0x0000, 0x0004, 0xFFFF,
    };
    const uint32_t lengths[] = {
        0, 1, 258, MAX_FRAME_PAYLOAD, MAX_FRAME_PAYLOAD + 1, 0xFFFFFFFF,
    };

    uint8_t   buf[12];
    uint64_t  state = 0x243F6A8885A308D3ULL;
    int       bad = 0;
    for (int i = 0; i < kIterations; ++i) {
        const size_t n = static_cast<size_t>(next_random(state) % (sizeof(buf) + 1));
        for (size_t j = 0; j < n; ++j) {
            buf[j] = static_cast<uint8_t>(next_random(state) >> 24);
        }
        // Plant structure often enough that the deep branches are actually
        // reached; pure random bytes almost never carry FRAME_MAGIC. Every
        // other input gets a complete, magic-correct header with a type and
        // length drawn from the interesting values; the rest stay random.
        if (n >= FRAME_HEADER_SIZE && (i % 2) == 0) {
            const uint16_t type = types[next_random(state) % 6];
            const uint32_t len  = lengths[next_random(state) % 6];
            const auto h = hdr(FRAME_MAGIC, type, len);
            for (size_t j = 0; j < FRAME_HEADER_SIZE; ++j) buf[j] = h[j];
        }
        if (const char* why = fuzz_check(buf, n)) {
            if (++bad == 1) {
                harness::expect(false, "random inputs satisfy every invariant",
                                std::string("iteration ") +
                                    std::to_string(i) + ": " + why);
            }
        }
    }
    if (bad == 0) {
        harness::expect(true, "random inputs satisfy every invariant",
                        std::string(std::to_string(kIterations) +
                                    " inputs, 0 failures"));
    }
}
#endif  // TCP_FUZZING

}  // namespace

// The libFuzzer entry point. Defined in both builds so the ordinary suite
// exercises the very same function clang will call; only main differs.
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (const char* why = fuzz_check(data, size)) {
        std::fprintf(stderr, "decode_header invariant: %s (size=%zu)\n",
                     why, size);
        std::abort();
    }
    return 0;
}

#ifndef TCP_FUZZING
int main() {
    check_table();
    check_random_inputs();
    return harness::finish();
}
#endif
