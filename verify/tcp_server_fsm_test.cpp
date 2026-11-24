// State-machine tests for conn_machine.
//
//   A. exhaustive closure: from every state valid() admits, every event (over a
//      grid of facts and parameters) lands in a state valid() admits, never
//      moves the phase backwards, and never leaves `closed`.
//   B. differential: the close decisions are compared, action and resulting
//      state, against the previous implementation (close_fsm), over every state
//      the new machine admits. The previous header is included inside a
//      separate namespace, so both exist in one binary.
//   C. lock-step random walk against the previous implementation, driven by the
//      events the reactor can actually issue, with the read/write flags checked
//      against the booleans they replaced.
//   D. the one deliberate difference: an exhausted input budget.
//   E. the owning class: published phase, send gating, the cross-thread
//      request channel.
//
// Build: g++ -std=c++17 -O1 -g -fsanitize=address,undefined -pthread
//            -DNEWH='"tcp-server-mt.h"'
//            -DOLDH='"tcp-server-mt.prev.h"' tcp_server_fsm_test.cpp
// (tcp-server-mt.prev.h is your previous header, kept for the differential tests)
// Pass --table to print the transition table instead of testing.
#include NEWH
namespace legacy {
#include OLDH
}
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

using namespace industrial;
namespace L = legacy::industrial;

static int  fails  = 0;
static long checks = 0;
#define CHECK(c) do { ++checks; if (!(c)) { ++fails; if (fails <= 25) std::printf("  FAIL line %d: %s\n", __LINE__, #c); } } while (0)

static const char* pname(conn_phase p) {
    switch (p) { case conn_phase::open: return "open"; case conn_phase::draining: return "draining";
                 case conn_phase::closing: return "closing"; case conn_phase::fin_sent: return "fin_sent";
                 case conn_phase::closed: return "closed"; }
    return "?";
}
static const char* rname(read_mode r) {
    switch (r) { case read_mode::active: return "active"; case read_mode::paused: return "paused"; case read_mode::eof: return "eof"; }
    return "?";
}
static const char* aname(close_action a) {
    switch (a) {
        case close_action::none: return "none"; case close_action::drain_input: return "drain_input";
        case close_action::drain_wbuf: return "drain_wbuf"; case close_action::drain_complete: return "drain_complete";
        case close_action::send_fin: return "send_fin"; case close_action::defer_drain_input: return "defer_drain_input";
        case close_action::defer_park: return "defer_park"; case close_action::defer_peer_closed: return "defer_peer_closed";
        case close_action::close_graceful: return "close_graceful"; case close_action::close_abort: return "close_abort";
    }
    return "?";
}

constexpr int64_t NOW = 1000;

struct Fx { bool wbuf_empty; bool rbuf_empty; size_t pending; };
static conn_facts mk(const Fx& f) {
    conn_facts x; x.wbuf_empty = f.wbuf_empty; x.rbuf_empty = f.rbuf_empty;
    x.rbuf_readable = f.rbuf_empty ? 0 : 10; x.pending = f.pending; return x;
}
static std::vector<Fx> all_facts() {
    std::vector<Fx> v;
    for (int w = 0; w < 2; ++w) for (int r = 0; r < 2; ++r) for (size_t p : {size_t(0), size_t(1)})
        v.push_back({bool(w), bool(r), p});
    return v;
}

static std::vector<conn_state> all_states(size_t* candidates = nullptr) {
    std::vector<conn_state> out; size_t cand = 0;
    for (int ph = 0; ph < 5; ++ph) for (int rd = 0; rd < 3; ++rd) for (int wa = 0; wa < 2; ++wa)
    for (int ir = 0; ir < 2; ++ir) for (int rn = 0; rn < 3; ++rn)
    for (int64_t cib : {int64_t(-1), int64_t(0), int64_t(1), int64_t(5000)})
    for (int64_t st : {int64_t(0), int64_t(100)}) for (int64_t pa : {int64_t(0), int64_t(50)}) {
        conn_state s; s.phase = conn_phase(ph); s.read = read_mode(rd); s.write_armed = wa;
        s.in_round = ir; s.round = close_round(rn); s.close_input_bytes = cib;
        s.close_started_at_ms = st; s.paused_at_ms = pa;
        ++cand;
        if (conn_sm::valid(s)) out.push_back(s);
    }
    if (candidates) *candidates = cand;
    return out;
}

// ------------------------------------------------------------------ A
static void test_closure() {
    std::puts("A. exhaustive closure");
    size_t cand = 0; auto states = all_states(&cand);
    std::printf("  %zu candidate field combinations, %zu admitted by valid()\n", cand, states.size());
    CHECK(!states.empty());
    size_t transitions = 0;
    auto landed = [&](const conn_state& before, const conn_state& after, const char* ev) {
        ++transitions;
        const char* why = conn_sm::violation(after);
        if (why) { std::printf("  closure broken by %s: %s (before phase=%s read=%s -> after phase=%s read=%s)\n", ev, why,
                               pname(before.phase), rname(before.read), pname(after.phase), rname(after.read)); }
        CHECK(why == nullptr);
        CHECK(after.phase >= before.phase);                      // phases only move forward
        if (before.phase == conn_phase::closed) CHECK(after.phase == conn_phase::closed);
        if (before.phase == conn_phase::fin_sent) CHECK(after.phase == conn_phase::fin_sent || after.phase == conn_phase::closed);
    };
    for (const auto& s0 : states) {
        for (bool arm : {false, true}) { conn_state s = s0; conn_sm::on_set_write(s, arm); landed(s0, s, "set_write"); }
        { conn_state s = s0; conn_sm::on_pause(s, NOW);   landed(s0, s, "pause"); }
        { conn_state s = s0; conn_sm::on_resume(s, NOW);  landed(s0, s, "resume"); }
        { conn_state s = s0; conn_sm::on_eof(s);          landed(s0, s, "eof"); }
        { conn_state s = s0; conn_sm::on_graceful(s, NOW);landed(s0, s, "graceful"); }
        { conn_state s = s0; conn_sm::on_round_end(s);    landed(s0, s, "round_end"); }
        { conn_state s = s0; conn_sm::on_closed(s);       landed(s0, s, "closed"); }
        for (const auto& f : all_facts()) {
            { conn_state s = s0; conn_sm::on_request(s, mk(f), NOW); landed(s0, s, "request"); }
            if (s0.read != read_mode::paused) { conn_state s = s0; conn_sm::on_round_begin(s); landed(s0, s, "round_begin"); }
            if (s0.read == read_mode::eof) { conn_state s = s0; conn_sm::on_peer_fin(s, mk(f)); landed(s0, s, "peer_fin"); }
            if (conn_sm::is_done(s0) || s0.close_input_bytes >= 0) {
                for (int served : {0, 1, 3}) for (int64_t dealt : {int64_t(0), int64_t(1), int64_t(6), int64_t(9000)}) {
                    conn_state s = s0; conn_sm::on_input_drained(s, mk(f), served, dealt); landed(s0, s, "input_drained");
                }
            }
            if (s0.close_input_bytes >= 0) { conn_state s = s0; conn_sm::on_input_defer_failed(s); landed(s0, s, "input_defer_failed"); }
        }
        if (s0.phase == conn_phase::closing) { conn_state s = s0; conn_sm::on_fin_sent(s); landed(s0, s, "fin_sent"); }
    }
    std::printf("  %zu transitions checked\n", transitions);

    // selected semantics, stated once as a table
    for (const auto& s0 : states) {
        { conn_state s = s0; bool ok = conn_sm::on_pause(s, NOW);
          const bool may = s0.read == read_mode::active &&
              (s0.phase == conn_phase::open || s0.phase == conn_phase::draining) && !s0.in_round;
          CHECK(ok == may); }
        { conn_state s = s0; bool first = conn_sm::on_closed(s); bool again = conn_sm::on_closed(s);
          CHECK(first == (s0.phase != conn_phase::closed)); CHECK(!again);
          CHECK(!s.write_armed && !s.in_round && s.read != read_mode::paused); }
        { conn_state s = s0; auto st = conn_sm::on_graceful(s, NOW);
          if (conn_sm::is_done(s0)) { CHECK(st.action == close_action::none); CHECK(s.phase == s0.phase); }
          else { CHECK(st.action == close_action::drain_wbuf); CHECK(s.phase == conn_phase::closing);
                 CHECK(st.resume.resumed == (s0.read == read_mode::paused && s0.phase != conn_phase::closing)); } }
        { conn_state s = s0; conn_sm::on_set_write(s, true);
          CHECK(s.write_armed == (s0.phase != conn_phase::closed)); }
        // interest: EPOLLIN only while reading
        CHECK(((conn_sm::interest(s0) & EPOLLIN) != 0) == (s0.read == read_mode::active));
        CHECK(((conn_sm::interest(s0) & EPOLLOUT) != 0) == s0.write_armed);
    }
}

// ------------------------------------------------------------------ B
struct LegacyWorld {
    L::close_fsm f; L::ring_buffer w, r; std::atomic<size_t> p{0};
    bool paused = false, peer = false, armed = false;
    LegacyWorld() { w.init(64); r.init(64); f.attach(&w, &r, &p); }
    void facts(const conn_facts& x) {
        w.clear(); r.clear(); uint8_t b[16] = {};
        if (!x.wbuf_empty) w.write(b, 4);
        if (!x.rbuf_empty) r.write(b, 10);
        p.store(x.pending);
    }
    void load(const conn_state& s) {
        f.closing.store(conn_sm::is_closing(s)); f.fin_sent = s.phase == conn_phase::fin_sent;
        f.is_closed.store(s.phase == conn_phase::closed);
        f.deferred_round = static_cast<L::close_fsm::deferred_kind>(static_cast<int>(s.round));
        f.close_input_bytes = s.close_input_bytes; f.draining_input = s.in_round;
        f.close_started_at_ms = s.close_started_at_ms;
        paused = s.read == read_mode::paused; peer = s.read == read_mode::eof; armed = s.write_armed;
    }
    // the reactor's caller guards, as they were
    bool pause() {
        if (f.closing.load() || f.draining_input) return false;
        if (paused || peer || f.is_closed.load()) return false;
        paused = true;
        return true;
    }
    bool resume() { if (!paused) return false; paused = false; return true; }
    bool set_write(bool a) { if (f.is_closed.load() || armed == a) return false; armed = a; return true; }
};
static close_action to_new(L::close_action a) { return static_cast<close_action>(static_cast<int>(a)); }
static int64_t expect_cib(int64_t before, int64_t legacy_after) { return before >= 0 ? std::max<int64_t>(0, legacy_after) : legacy_after; }

static bool same_close_state(const conn_state& n, const LegacyWorld& lw, int64_t cib_before) {
    return conn_sm::is_closing(n) == lw.f.closing.load() && (n.phase == conn_phase::fin_sent) == lw.f.fin_sent &&
           (n.phase == conn_phase::closed) == lw.f.is_closed.load() &&
           static_cast<int>(n.round) == static_cast<int>(lw.f.deferred_round) &&
           n.close_input_bytes == expect_cib(cib_before, lw.f.close_input_bytes) &&
           n.in_round == lw.f.draining_input &&
           (n.close_started_at_ms != 0) == (lw.f.close_started_at_ms != 0);
}

static void test_differential() {
    std::puts("B. differential against the previous close_fsm");
    auto states = all_states(); size_t compared = 0;
    for (const auto& s0 : states) for (const auto& f : all_facts()) {
        const conn_facts cf = mk(f);
        { // graceful
            conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
            auto st = conn_sm::on_graceful(s, NOW); auto la = lw.f.on_graceful();
            CHECK(st.action == to_new(la)); CHECK(same_close_state(s, lw, s0.close_input_bytes));
            const bool lres = la != L::close_action::none && lw.resume();
            CHECK(st.resume.resumed == lres); ++compared;
        }
        { // request
            conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
            auto st = conn_sm::on_request(s, cf, NOW); auto la = lw.f.on_request();
            CHECK(st.action == to_new(la)); CHECK(same_close_state(s, lw, s0.close_input_bytes));
            const bool lres = la != L::close_action::none && lw.resume();
            CHECK(st.resume.resumed == lres); ++compared;
        }
        if (conn_sm::is_done(s0) || s0.close_input_bytes >= 0) {
            for (int served : {0, 1, 3}) for (int64_t dealt : {int64_t(0), int64_t(1), int64_t(6), int64_t(9000)}) {
                conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
                auto a = conn_sm::on_input_drained(s, cf, served, dealt); auto la = lw.f.on_input_drained(served, dealt);
                CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.close_input_bytes)); ++compared;
            }
        }
        if (s0.close_input_bytes >= 0) {
            conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
            auto a = conn_sm::on_input_defer_failed(s); auto la = lw.f.on_input_defer_failed();
            CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.close_input_bytes)); ++compared;
        }
        if (s0.read == read_mode::eof) {
            conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
            auto a = conn_sm::on_peer_fin(s, cf); auto la = lw.f.on_peer_fin();
            CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.close_input_bytes)); ++compared;
        }
    }
    for (auto r : {drain_outcome::drained, drain_outcome::stalled, drain_outcome::dead, drain_outcome::error}) {
        L::close_fsm lf; CHECK(conn_sm::on_drain_result(r) == to_new(lf.on_drain_result(static_cast<L::drain_outcome>(static_cast<int>(r)))));
    }
    // the read/write flags against the booleans they replaced
    for (const auto& s0 : states) {
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          const bool nr = conn_sm::on_pause(s, NOW), lr = lw.pause();
          CHECK(nr == lr);
          if (s0.phase != conn_phase::closed)
              CHECK((s.read == read_mode::paused) == lw.paused);
          ++compared;
        }
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          const bool nr = conn_sm::on_resume(s, NOW).resumed, lr = lw.resume(); CHECK(nr == lr); ++compared; }
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          for (bool a : {true, false, true}) { CHECK(conn_sm::on_set_write(s, a) == lw.set_write(a)); CHECK(s.write_armed == lw.armed); } ++compared; }
        { uint32_t legacy_interest =
              (!(s0.read == read_mode::paused) && !(s0.read == read_mode::eof)
                   ? static_cast<uint32_t>(EPOLLIN) : 0u) |
              (s0.write_armed ? static_cast<uint32_t>(EPOLLOUT) : 0u);
          CHECK(conn_sm::interest(s0) == legacy_interest); }
    }
    std::printf("  %zu decisions compared, all agree (modulo test D)\n", compared);
}

// ------------------------------------------------------------------ C
static void test_random_walk(unsigned seed, int steps) {
    std::mt19937 rng(seed);
    auto rnd = [&](int n) { return int(rng() % unsigned(n)); };
    conn_state s; LegacyWorld lw;
    long applied = 0, resets = 0; long phase_visits[5] = {};
    auto sync = [&](const conn_facts& cf) { lw.facts(cf); };
    auto compare = [&](int64_t cib_before, const char* what) {
        CHECK(conn_sm::valid(s));
        if (!conn_sm::valid(s)) { std::printf("  walk seed %u step state invalid after %s: %s\n", seed, what, conn_sm::violation(s)); }
        CHECK((s.phase == conn_phase::closed) == lw.f.is_closed.load());
        if (s.phase != conn_phase::closed) {
            // After teardown the old encoding is stale by design (the reactor cleared `closing` in
            // unregister_conn and left fin_sent alone); only live connections are comparable.
            CHECK(conn_sm::is_closing(s) == lw.f.closing.load());
            CHECK((s.phase == conn_phase::fin_sent) == lw.f.fin_sent);
        }
        CHECK(static_cast<int>(s.round) == static_cast<int>(lw.f.deferred_round));
        CHECK(s.close_input_bytes == expect_cib(cib_before, lw.f.close_input_bytes));
        if (s.phase != conn_phase::closed) {
            CHECK(s.in_round == lw.f.draining_input);   // the machine clears it at teardown; the flag outlived it
            CHECK((s.read == read_mode::paused) == lw.paused);
            CHECK((s.read == read_mode::eof) == lw.peer);
            CHECK(s.write_armed == lw.armed);
        }
        // draining == "closing false but a budget measured" in the old encoding
        CHECK((s.phase == conn_phase::draining) == (!lw.f.closing.load() && !lw.f.is_closed.load() && lw.f.close_input_bytes >= 0));
        ++phase_visits[int(s.phase)];
    };
    // the old code kept the budget in one signed field; mirror its trajectory
    auto lcib_sync = [&] { lw.f.close_input_bytes = s.close_input_bytes; };

    // finish_peer_closed()'s decision, applied to both machines
    auto peer_fin_effects = [&](const conn_facts& cf) {
        if (s.read != read_mode::eof || s.phase == conn_phase::closed) return;
        if (!cf.wbuf_empty) { conn_sm::on_set_write(s, true); lw.set_write(true); return; }   // parked: handle_write resumes it
        const int64_t before = s.close_input_bytes;
        auto a = conn_sm::on_peer_fin(s, cf); auto la = lw.f.on_peer_fin();
        CHECK(a == to_new(la));
        if (a == close_action::send_fin) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; }
        if (a == close_action::send_fin || a == close_action::close_graceful) {
            if (a == close_action::close_graceful) { conn_sm::on_closed(s); lw.f.is_closed.store(true); }
        }
        compare(before, "peer_fin");
    };

    for (int i = 0; i < steps; ++i) {
        conn_facts cf; cf.wbuf_empty = rnd(3) != 0; cf.rbuf_empty = rnd(2) == 0; cf.rbuf_readable = cf.rbuf_empty ? 0 : 10; cf.pending = rnd(2) ? 0 : 5;
        sync(cf);
        const int64_t before = s.close_input_bytes;
        const int ev = rnd(12);
        switch (ev) {
        case 0: { CHECK(conn_sm::on_pause(s, NOW + i) == lw.pause()); compare(before, "pause"); break; }
        case 1: { const bool was_closed = s.phase == conn_phase::closed;
                  const bool nr = conn_sm::on_resume(s, NOW + i).resumed, lr = lw.resume();
                  if (!was_closed) CHECK(nr == lr);   // a closed connection is normalised; the old flag was not
                  compare(before, "resume"); break; }
        case 2: { if (s.read == read_mode::active && s.phase != conn_phase::closed) { conn_sm::on_eof(s); lw.peer = true; compare(before, "eof"); } break; }
        case 3: { if (s.phase != conn_phase::closed) { bool a = rnd(2); CHECK(conn_sm::on_set_write(s, a) == lw.set_write(a)); compare(before, "set_write"); } break; }
        case 4: { auto st = conn_sm::on_graceful(s, NOW + i); auto la = lw.f.on_graceful();
                  CHECK(st.action == to_new(la)); bool lres = la != L::close_action::none && lw.resume();
                  CHECK(st.resume.resumed == lres); compare(before, "graceful"); break; }
        case 5: case 6: {   // drain_input_then_close(), including the hand-offs that can fail
            auto st = conn_sm::on_request(s, cf, NOW + i); auto la = lw.f.on_request();
            CHECK(st.action == to_new(la)); bool lres = la != L::close_action::none && lw.resume();
            CHECK(st.resume.resumed == lres); compare(before, "request");
            if (st.action == close_action::none) break;
            conn_sm::on_round_begin(s); lw.f.draining_input = true; compare(s.close_input_bytes, "round_begin");
            const int inner = rnd(5);   // what handle_read did meanwhile
            if (inner == 1 && s.read == read_mode::active) { conn_sm::on_eof(s); lw.peer = true; peer_fin_effects(cf); }
            if (inner == 2 && s.phase != conn_phase::closed) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; }
            conn_sm::on_round_end(s); lw.f.draining_input = false;
            if (s.phase == conn_phase::closed || s.phase == conn_phase::fin_sent) { compare(s.close_input_bytes, "round_end"); break; }
            const int served = rnd(4);
            // keep dealt within the budget so the old field never goes negative (test D covers that case)
            const int64_t dealt = served > 0 ? int64_t(rnd(int(std::min<int64_t>(s.close_input_bytes, 40)) + 1)) : 0;
            const int64_t cb = s.close_input_bytes;
            close_action a = conn_sm::on_input_drained(s, cf, served, dealt); auto la2 = lw.f.on_input_drained(served, dealt);
            CHECK(a == to_new(la2)); compare(cb, "input_drained");
            if (a == close_action::defer_drain_input && rnd(3) == 0) {          // hand-off failed
                const int64_t cb2 = s.close_input_bytes;
                a = conn_sm::on_input_defer_failed(s); auto la3 = lw.f.on_input_defer_failed();
                CHECK(a == to_new(la3)); compare(cb2, "defer_failed");
            }
            if (a == close_action::drain_wbuf || (a == close_action::defer_park && rnd(3) == 0)) {   // park_close
                auto g = conn_sm::on_graceful(s, NOW + i); auto lg = lw.f.on_graceful();
                CHECK(g.action == to_new(lg)); bool lr2 = lg != L::close_action::none && lw.resume();
                CHECK(g.resume.resumed == lr2); compare(s.close_input_bytes, "park");
            }
            break; }
        case 7: { peer_fin_effects(cf); break; }
        case 8: {   // handle_write: a drain, then disarm and maybe FIN
            if (s.phase == conn_phase::closed || s.phase == conn_phase::fin_sent) break;
            const drain_outcome r[] = {drain_outcome::drained, drain_outcome::stalled, drain_outcome::dead, drain_outcome::error};
            const drain_outcome o = r[rnd(4)];
            const close_action a = conn_sm::on_drain_result(o);
            if (a == close_action::close_abort) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; compare(before, "abort"); break; }
            if (a == close_action::none) break;
            if (s.read == read_mode::eof) { peer_fin_effects(cf); break; }
            conn_sm::on_set_write(s, false); lw.set_write(false);
            if (conn_sm::is_closing(s) && s.phase == conn_phase::closing) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; }
            compare(before, "handle_write"); break; }
        case 9: { if (rnd(6) == 0) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; compare(before, "closed"); } break; }
        default: { if (s.phase == conn_phase::closing && cf.wbuf_empty) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; compare(before, "fin"); } break; }
        }
        ++applied;
        if (s.phase == conn_phase::closed && rnd(4) == 0) { s = conn_state(); lw.f.closing.store(false); lw.f.fin_sent = false; lw.f.is_closed.store(false);
            lw.f.deferred_round = L::close_fsm::deferred_kind::none; lw.f.close_input_bytes = -1; lw.f.draining_input = false; lw.f.close_started_at_ms = 0;
            lw.paused = lw.peer = lw.armed = false; ++resets; }
        (void)lcib_sync;
    }
    if (seed == 1) std::printf("  seed 1: %ld events over %ld connection lifetimes; phase visits open=%ld draining=%ld closing=%ld fin_sent=%ld closed=%ld\n",
                               applied, resets + 1, phase_visits[0], phase_visits[1], phase_visits[2], phase_visits[3], phase_visits[4]);
}

// ------------------------------------------------------------------ D
static void test_exhausted_budget() {
    std::puts("D. exhausted input budget (the deliberate difference)");
    conn_facts cf; cf.rbuf_empty = false; cf.rbuf_readable = 700; cf.pending = 0; cf.wbuf_empty = true;
    // new
    conn_state s;
    conn_sm::on_request(s, cf, NOW);                           // measures 700
    CHECK(s.close_input_bytes == 700);
    // the round consumed 700 buffered bytes plus 300 that arrived during it
    cf.rbuf_readable = 10; cf.rbuf_empty = false;
    auto a = conn_sm::on_input_drained(s, cf, 5, 1000);
    CHECK(s.close_input_bytes == 0);                           // exhausted, not "unmeasured"
    CHECK(a != close_action::defer_drain_input);
    conn_sm::on_request(s, cf, NOW + 5);                       // a second close request arrives
    CHECK(s.close_input_bytes == 0);                           // still exhausted: no fresh budget
    // legacy
    LegacyWorld lw; lw.facts(conn_facts{true, false, 700, 0});
    lw.f.on_request(); lw.facts(conn_facts{true, false, 10, 0});
    lw.f.on_input_drained(5, 1000);
    const int64_t exhausted = lw.f.close_input_bytes;
    lw.f.on_request();
    std::printf("  old: budget after the round = %lld, after a second close request = %lld (re-measured from rbuf)\n",
                (long long)exhausted, (long long)lw.f.close_input_bytes);
    std::printf("  new: budget after the round = 0, after a second close request = %lld\n", (long long)s.close_input_bytes);
    CHECK(exhausted < 0);
    CHECK(lw.f.close_input_bytes == 10);                       // the old code handed out a fresh budget
}

// ------------------------------------------------------------------ E
static void test_machine_class() {
    std::puts("E. conn_machine: publication and the cross-thread channel");
    ring_buffer wb, rb; wb.init(64); rb.init(64); std::atomic<size_t> pending{0};
    conn_machine m; m.attach(&wb, &rb, &pending);
    CHECK(m.published_phase() == conn_phase::open && !m.closing() && !m.closed() && !m.refuses_sends());
    CHECK(m.on_pause(NOW)); CHECK(m.read_paused());
    CHECK(m.interest() == 0);
    CHECK(m.on_set_write(true)); CHECK(m.interest() == EPOLLOUT);
    uint8_t b[10] = {}; rb.write(b, 10); wb.write(b, 4); pending.store(3);
    auto st = m.on_request(NOW + 10);                         // close requested while paused
    CHECK(st.action == close_action::drain_input); CHECK(st.resume.resumed); CHECK(st.resume.paused_for_ms == 10);
    CHECK(!m.read_paused()); CHECK(m.state().phase == conn_phase::draining); CHECK(m.state().close_input_bytes == 10);
    CHECK(m.published_phase() == conn_phase::draining);
    CHECK(!m.refuses_sends());                                 // replies are still accepted while draining
    CHECK(m.interest() == (EPOLLIN | EPOLLOUT));
    m.on_round_begin(); CHECK(m.state().in_round); CHECK(!m.on_pause(NOW + 11)); CHECK(!m.backpressure_applies()); m.on_round_end();
    CHECK(m.backpressure_applies());
    auto g = m.on_graceful(NOW + 20);
    CHECK(g.action == close_action::drain_wbuf); CHECK(!g.resume.resumed);
    CHECK(m.closing() && m.refuses_sends() && !m.closed() && !m.fin_sent());
    CHECK(m.close_started_at_ms() == NOW + 20);
    CHECK(!m.on_pause(NOW + 21));                              // a parked connection is never paused
    wb.clear(); m.on_fin_sent(); CHECK(m.fin_sent() && m.closing());
    CHECK(m.on_closed()); CHECK(!m.on_closed()); CHECK(m.closed() && m.refuses_sends());
    CHECK(!m.write_armed());
    CHECK(!m.on_set_write(true));                              // write interest means nothing once gone
    // request channel
    conn_machine n; CHECK(!n.close_request_pending());
    n.note_close_request(); CHECK(n.close_request_pending());
    CHECK(!n.take_close_wanted()); n.post_close_wanted(); CHECK(n.take_close_wanted()); CHECK(!n.take_close_wanted());
    // a connection that is rejected before it served
    conn_machine r; r.attach(&wb, &rb, &pending); r.on_rejected(); CHECK(r.closed());
    // sends are gated on exactly the phases that park or end the connection
    for (conn_phase p : {conn_phase::open, conn_phase::draining, conn_phase::closing, conn_phase::fin_sent, conn_phase::closed}) {
        conn_machine x; x.attach(&wb, &rb, &pending);
        if (p >= conn_phase::draining) x.on_request(NOW);
        if (p >= conn_phase::closing) x.on_graceful(NOW);
        if (p >= conn_phase::fin_sent) x.on_fin_sent();
        if (p >= conn_phase::closed) x.on_closed();
        CHECK(x.published_phase() == p);
        CHECK(x.refuses_sends() == (p >= conn_phase::closing));
        CHECK(x.closing() == (p == conn_phase::closing || p == conn_phase::fin_sent));
    }
}

// ------------------------------------------------------------------ table
static void print_table() {
    std::puts("| state (phase, read) | event | next (phase, read) | action | reactor must also |");
    std::puts("|---|---|---|---|---|");
    const conn_facts quiet = mk({true, true, 0});
    const conn_facts owed  = mk({true, true, 1});
    struct Row { conn_state s; const char* tag; };
    std::vector<Row> rows;
    auto add = [&](conn_phase p, read_mode r, int64_t cib, int64_t st, int64_t pa, close_round rn, const char* tag) {
        conn_state s; s.phase = p; s.read = r; s.close_input_bytes = cib; s.close_started_at_ms = st; s.paused_at_ms = pa; s.round = rn;
        if (conn_sm::valid(s)) rows.push_back({s, tag});
    };
    add(conn_phase::open, read_mode::active, -1, 0, 0, close_round::none, "");
    add(conn_phase::open, read_mode::paused, -1, 0, 50, close_round::none, "");
    add(conn_phase::open, read_mode::eof, -1, 0, 0, close_round::none, "");
    add(conn_phase::draining, read_mode::active, 10, 0, 0, close_round::none, "");
    add(conn_phase::closing, read_mode::active, 10, 100, 0, close_round::none, "");
    add(conn_phase::fin_sent, read_mode::active, 10, 100, 0, close_round::none, "");
    add(conn_phase::closed, read_mode::active, 10, 100, 0, close_round::none, "");
    auto lbl = [](const conn_state& s) { return std::string(pname(s.phase)) + ", " + rname(s.read); };
    auto line = [&](const conn_state& a, const char* ev, const conn_state& b, const char* act, const char* extra) {
        std::printf("| %s | %s | %s | %s | %s |\n", lbl(a).c_str(), ev, lbl(b).c_str(), act, extra);
    };
    for (auto& r : rows) {
        { conn_state s = r.s; bool ok = conn_sm::on_pause(s, NOW); line(r.s, "pause (backpressure)", s, ok ? "(paused)" : "refused", ok ? "drop EPOLLIN" : ""); }
        { conn_state s = r.s; auto x = conn_sm::on_resume(s, NOW); line(r.s, "resume", s, x.resumed ? "(resumed)" : "none", x.resumed ? "restore EPOLLIN; shift frame deadline; schedule if rbuf non-empty" : ""); }
        { conn_state s = r.s; auto x = conn_sm::on_graceful(s, NOW); line(r.s, "graceful close / idle", s, aname(x.action), x.resume.resumed ? "apply the implied resume; drain wbuf" : (x.action == close_action::none ? "" : "drain wbuf")); }
        { conn_state s = r.s; auto x = conn_sm::on_request(s, quiet, NOW); line(r.s, "close_connection (request)", s, aname(x.action), x.action == close_action::none ? "" : "apply the implied resume; run a serving round"); }
        { conn_state s = r.s; conn_sm::on_eof(s); line(r.s, "recv() == 0", s, "(eof)", "finish_peer_closed"); }
        if (r.s.read == read_mode::eof) {
            { conn_state s = r.s; auto a = conn_sm::on_peer_fin(s, quiet); line(r.s, "peer FIN, wbuf empty, nothing owed", s, aname(a), ""); }
            { conn_state s = r.s; auto a = conn_sm::on_peer_fin(s, owed);  line(r.s, "peer FIN, wbuf empty, replies owed", s, aname(a), a == close_action::defer_peer_closed ? "queue one round" : ""); }
        }
        if (r.s.phase == conn_phase::closing) { conn_state s = r.s; conn_sm::on_fin_sent(s); line(r.s, "shutdown(SHUT_WR) ok", s, "(fin sent)", ""); }
        { conn_state s = r.s; bool first = conn_sm::on_closed(s); line(r.s, "teardown", s, first ? "(closed)" : "none", first ? "close fd, account, deferred callback" : ""); }
    }
}

int main(int argc, char** argv) {
    if (argc > 1 && std::strcmp(argv[1], "--table") == 0) { print_table(); return 0; }
    test_closure();
    test_differential();
    std::puts("C. lock-step random walks against the previous close_fsm");
    for (unsigned seed = 1; seed <= 40; ++seed) test_random_walk(seed, 20000);
    std::printf("  40 walks x 20000 events, all agree\n");
    test_exhausted_budget();
    test_machine_class();
    std::printf("\n%ld checks: %s (%d failures)\n", checks, fails ? "FAILED" : "ALL PASSED", fails);
    return fails != 0;
}
