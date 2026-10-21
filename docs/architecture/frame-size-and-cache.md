# `FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD` and the cache

The question: `8 + 65536 = 65544`, eight bytes past sixteen pages (65536). Does that little
excess cost anything in cache?

The answer: **no, because those eight bytes never reach the cache.** Measurement:
`verify/evidence/probe_stride_cache.cpp`.

---

## 1. The allocator eats the +8 first

Connection buffers are `std::vector<uint8_t>`, so they go through `operator new`. A glibc
chunk has to be 16-byte aligned and carries an 8-byte header:

| Requested capacity | chunk = cap + 8 | aligned to 16 | measured stride |
|---|---|---|---|
| 65544 | 65552 | 65552 | **65552** |
| 65536 | 65544 | 65552 | **65552** |
| 65600 | 65608 | 65616 | 65616 |

`cap=65544` and `cap=65536` produce **exactly the same stride**. The eight bytes are
absorbed precisely by 16-byte alignment, and the cache sees an identical address sequence.

So the premise — "slightly more than a page" — does not hold at the cache level. It is not
"slightly more"; it is "the same".

## 2. And neither case aliases

On x86-64 the L1 set index is address bits `[11:6]`. When the stride is a multiple of 4096,
every buffer lands on the same set: with 8-way associativity that is 8 lines resident and
the rest evicted on every pass.

The measured stride is 65552, and `65552 % 4096 = 16` — not a page multiple. The set index
for buffer k advances by `floor(k·0.25)`, so 256 buffers cover all 64 sets. **No aliasing.**

One thing worth remembering: **asking for a tidy 64 KiB capacity buys no alignment**,
because the allocator rewrites it to 65552 anyway. Optimising in that direction is wasted
work.

## 3. What the eight bytes actually buy

`MAX_FRAME_PAYLOAD` is 65536, so a full frame is an 8-byte header plus 65536 bytes of
payload = 65544. The buffer holds exactly one full frame.

Rounding the capacity down to 65536 would make the largest possible reply impossible to
write — on an idle connection, forever. The comment on `Config::wbufCapacity` already says
this; it calls the value a floor rather than a coincidence.

## 4. The one real cache artefact: adjacent buffers share a line

`65552 % 64 = 16`. The last 16 bytes of buffer k and the first 48 bytes of buffer k+1 sit on
the same 64-byte line. Two reactor threads writing that line concurrently is false sharing.

Measured (two threads, 2 million writes each, same line versus lines a row apart):

| Case | ns/op |
|---|---|
| Same cache line | 2.01 |
| Different cache lines | 1.34 |

**Sharing the line is about 50% slower per operation.**

But that cost almost never materialises on the real path: `rbuf` is written sequentially, and
the boundary line is only touched when the ring wraps to the end — once per 64 KiB of
traffic. Same for `wbuf`. So it is real, and it almost never fires.

One counterexample, and the place this probe nearly fooled me: if the two buffers are
allocated with two separate `emplace_back` calls, the heap leaves a gap between them
(measured stride 65616 rather than 65552), and the two pointers are not on the same line at
all — the test still "passes", having measured nothing. Section C only became real once it
allocated one block and sliced it at the stride.

## 5. Page count

If it were page-aligned, 65544 bytes would indeed span 17 pages. But the buffer comes from
the heap, not from one `mmap` per connection: the seventeenth page is shared with the next
chunk.

Measured RSS: **128.70 KB per connection**, unchanged across every change from R3 through
R5. No extra 4 KiB.

---

## Recommendation

**Leave it alone.** Nothing measured supports changing it.

If you want cache-line alignment for its own sake, `65600` (the next multiple of 64) costs
56 bytes per connection, 560 KB at ten thousand connections. But the probe's `65600` row
being faster (1.62 vs 2.12 ns/line) is something I am **not willing to use as a reason** —
strides 65616 and 65552 show no structural difference in set distribution, the gap looks
like an artefact of heap layout, and it does not reproduce at server scale.

Changing this number requires a benchmark that can resolve it first. There isn't one.
