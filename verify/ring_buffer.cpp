// verify/ring_buffer.cpp
//
// RingBuffer::reserve() exists so that the write side can be tested without a
// socket. Before it, filling the buffer in place took three public methods --
// writePtr(), writeSpan(), commit() -- and the contract between them (only the
// contiguous part is offered; a span of 0 means "wrapped, try later"; nothing
// counts until commit) lived in handleRead()'s head.
//
// This suite asserts that contract directly. It needs no epoll, no thread and
// no peer, which is the point: the same assertions used to require a running
// server and a peer that happened to write at the right moment.
//
//     g++ -std=c++17 -Wall -Wextra -Iinclude verify/ring_buffer.cpp

#include "tcp-server-mt.h"

#include <cstdio>
#include <cstring>

namespace {

using industrial::detail::RingBuffer;

int g_pass = 0;
int g_fail = 0;

void check(const char* what, bool ok, const char* detail = nullptr) {
    if (ok) {
        ++g_pass;
    } else {
        ++g_fail;
        std::printf("  FAIL  %s%s%s\n", what, detail ? "  " : "",
                    detail ? detail : "");
    }
}

}  // namespace

int main() {
    // ---- 1. an empty ring offers its whole capacity in one piece -----------
    {
        RingBuffer rb(16);
        auto slot = rb.reserve(16);
        check("empty ring reserves the whole capacity", slot.size() == 16);
        check("and reports usable", static_cast<bool>(slot) == true);
        check("and points at the buffer", slot.data() != nullptr);
    }

    // ---- 2. a wrapped ring offers only the contiguous part ------------------
    // head=8, size=4, so the tail sits at 12 and only 4 bytes run to the end.
    // This is the case the old writeSpan() reported as 0-or-short and that
    // handleRead had to interpret.
    {
        RingBuffer rb(16);
        uint8_t src[16] = {0};
        rb.write(src, 12);
        rb.consume(8);
        check("wrapped ring: readable is what is left", rb.readable() == 4);
        auto slot = rb.reserve(16);
        check("wrapped ring reserves only to the end", slot.size() == 4);
        // Draining it does not help: head has advanced to 12, so an *empty*
        // ring still offers only the four bytes that run to the end. This is
        // the fact that made the old API hard to use -- "empty" and "fully
        // contiguous" are not the same thing.
        rb.consume(4);
        check("empty, but head is at 12", rb.readable() == 0);
        auto slot2 = rb.reserve(16);
        check("an empty ring can still offer only the tail", slot2.size() == 4);
        // Write those four and consume them: head comes back to 0 and only
        // then is the whole capacity contiguous again.
        uint8_t four[4] = {0};
        rb.write(four, 4);
        rb.consume(4);
        auto slot3 = rb.reserve(16);
        check("head back at zero offers the whole capacity",
              slot3.size() == 16);
    }

    // ---- 3. a slot that is never committed writes nothing -------------------
    // The mistake the old API allowed: take the pointer, write, forget commit.
    {
        RingBuffer rb(16);
        {
            auto slot = rb.reserve(16);
            std::memset(slot.data(), 0xAB, slot.size());
            // no commit
        }
        check("uncommitted slot leaves readable at 0", rb.readable() == 0);
        check("uncommitted slot leaves the buffer writable", rb.writable() == 16);
    }

    // ---- 4. committing more than was reserved is clamped --------------------
    // Under the old commit(), the clamp was against writable(), so a caller
    // could count bytes it never owned -- past the wrap point, into the region
    // another read would fill.
    {
        RingBuffer rb(16);
        uint8_t src[16] = {0};
        rb.write(src, 12);
        rb.consume(8);
        auto slot = rb.reserve(16);            // 4 bytes, contiguous only
        slot.commit(9);                        // more than it was given
        check("over-commit is clamped to the slot", rb.readable() == 4 + 4);
    }

    // ---- 5. a full ring offers nothing -------------------------------------
    {
        RingBuffer rb(16);
        uint8_t src[16] = {0};
        rb.write(src, 16);
        auto slot = rb.reserve(16);
        check("full ring reserves nothing", slot.size() == 0);
        check("and is falsy", static_cast<bool>(slot) == false);
    }

    // ---- 6. write() still spans the wrap point in one call -----------------
    // reserve() is contiguous-only on purpose, but write() is not, and that
    // difference is load-bearing: submitSend checks writable() >= frame and
    // then expects the whole frame to land. If write() had been reimplemented
    // on top of reserve() it would silently truncate every frame that straddled
    // the boundary.
    {
        RingBuffer rb(16);
        uint8_t src[16] = {0};
        rb.write(src, 12);
        rb.consume(10);                        // head=10, size=2, tail=12
        uint8_t more[8];
        std::memset(more, 0x5A, sizeof(more));
        const size_t w = rb.write(more, 8);
        check("write() crosses the wrap point", w == 8);
        check("and all 8 bytes are readable", rb.readable() == 10);
        // The 8 bytes must be contiguous in logical order across the boundary.
        uint8_t out[10] = {0};
        rb.peek(out, 10);
        bool same = true;
        for (int i = 2; i < 10; ++i) {
            if (out[i] != 0x5A) same = false;
        }
        check("and the payload survives the wrap", same);
    }

    std::printf("\n%s  pass=%d fail=%d\n", g_fail == 0 ? "PASS" : "FAIL",
                g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
