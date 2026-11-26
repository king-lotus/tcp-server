// State-machine tests for the closed-enum connection machine.
//
//   1. states      the 77 states are re-derived here, independently, from the
//                  five discrete rules, and compared with the generated enum:
//                  same set, per-phase counts, accessors, names, dense ids.
//   2. cells       every generated cell (77 states x events x guards, the
//                  in-domain ones) against the shipped transition(); every cell
//                  not listed is inert; phases only move forward and `closed`
//                  is terminal.
//   3. payloads    every cell applied through conn_sm::step() with real
//                  payloads: the stamp, the budget, the resume report.
//   4. oracle      the whole new machine (generated table + payload plumbing +
//                  guard classification) against the five-axis oracle in
//                  tools/conn_fsm_spec.h, over every state x event x
//                  combination of the facts the decisions read.
//   5. previous    the close decisions against the previous implementation
//                  (close_fsm in tcp-server-mt.prev.h), over every state.
//   6. walks       lock-step random walks against the previous implementation,
//                  driven by the events the reactor can actually issue.
//   7. budget      the one deliberate difference from the previous
//                  implementation: an exhausted input budget.
//   8. machine     the owning class: publication and the cross-thread channel.
//
// There is no "valid after every transition" test: a state the machine cannot
// be in has no enumerator.
//
// Build: verify/run.sh compiles this file with -Iinclude -Iverify and
//   -DNEWH='"tcp-server-mt.h"' -DOLDH='"tcp-server-mt.prev.h"', so the machine
//   under test and the branch-point oracle live in one binary.
#include NEWH
#include "../tools/conn_fsm_model.h"
namespace legacy {
#include OLDH
}
#include <cstdio>
#include <cstring>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <vector>

using namespace industrial;
namespace L = legacy::industrial;
namespace M = model;

#include "conn_fsm_cases.inc"

static int  fails = 0;
static long checks = 0;
#define CHECK(c) do { ++checks; if (!(c)) { ++fails; if (fails <= 25) std::printf("  FAIL line %d: %s\n", __LINE__, #c); } } while (0)

constexpr int64_t NOW = 1000;
constexpr size_t kStates = sizeof(kStateRows) / sizeof(kStateRows[0]);

static bool closing_phase(conn_phase p) { return p == conn_phase::closing || p == conn_phase::fin_sent; }

// A conn_state for an id with payloads that are consistent for it.
static conn_state make(conn_state_id id, int64_t budget = -1) {
    conn_state s;
    s.id = id;
    const conn_phase p = phase_of(id);
    if (p == conn_phase::open) budget = -1;
    if (p == conn_phase::draining && budget < 0) budget = 500;
    s.budget = budget;
    s.stamp_ms = read_of(id) == read_mode::paused ? 7000 : (closing_phase(p) ? 8000 : 0);
    return s;
}
static M::Axes axes_of_id(conn_state_id id) {
    M::Axes a;
    a.phase = phase_of(id); a.read = read_of(id); a.write = write_armed_of(id);
    a.in_round = in_round_of(id); a.round = round_of(id);
    return a;
}
static std::vector<int64_t> budgets_for(conn_state_id id, bool measured_only) {
    return M::budget_variants(axes_of_id(id), measured_only ? M::E::input_drained : M::E::pause);
}

// ------------------------------------------------------------------ 1
// The five discrete rules, restated here so the generated enum is checked
// against something the generator did not produce.
static bool five_rules_admit(conn_phase ph, read_mode rd, bool write, bool in_round, close_round rn) {
    if (rn == close_round::close && ph == conn_phase::open) return false;
    if (rn == close_round::peer_closed && rd != read_mode::eof) return false;
    if (rd == read_mode::paused && ((ph != conn_phase::open && ph != conn_phase::draining) || in_round)) return false;
    if (ph == conn_phase::closed && (write || in_round || rd == read_mode::paused)) return false;
    if (in_round && ph == conn_phase::open) return false;
    return true;
}

static void test_states() {
    std::puts("1. the 77 states");
    CHECK(k_conn_state_count == 77);
    CHECK(kStates == 77);
    std::set<std::tuple<int, int, int, int, int>> have;
    int per[5] = {0, 0, 0, 0, 0};
    std::set<std::string> names;
    for (size_t i = 0; i < kStates; ++i) {
        const state_row& r = kStateRows[i];
        CHECK(static_cast<size_t>(r.id) == i);                          // dense, ordered
        CHECK(phase_of(r.id) == r.phase && read_of(r.id) == r.read && write_armed_of(r.id) == r.write_armed &&
              in_round_of(r.id) == r.in_round && round_of(r.id) == r.round);
        CHECK(std::strcmp(name_of(r.id), r.name) == 0);
        CHECK(names.insert(r.name).second);
        ++per[static_cast<int>(r.phase)];
        have.insert(std::make_tuple(int(r.phase), int(r.read), int(r.write_armed) * 2 + int(r.in_round), int(r.round), 0));
    }
    // exactly the combinations the five rules admit
    size_t admitted = 0; bool same = true;
    for (int p = 0; p < 5; ++p) for (int r = 0; r < 3; ++r) for (int w = 0; w < 2; ++w)
    for (int ir = 0; ir < 2; ++ir) for (int rn = 0; rn < 3; ++rn) {
        const bool ok = five_rules_admit(conn_phase(p), read_mode(r), w, ir, close_round(rn));
        const bool listed = have.count(std::make_tuple(p, r, w * 2 + ir, rn, 0)) != 0;
        if (ok != listed) same = false;
        admitted += ok;
    }
    CHECK(same);
    CHECK(admitted == 77);
    CHECK(per[0] == 8 && per[1] == 24 && per[2] == 20 && per[3] == 20 && per[4] == 5);
    std::printf("  %zu of 180 combinations admitted by the five rules == the %zu enumerators; per phase %d/%d/%d/%d/%d\n",
                admitted, kStates, per[0], per[1], per[2], per[3], per[4]);
    // the rare legal states the design keeps
    CHECK(five_rules_admit(conn_phase::fin_sent, read_mode::active, false, true, close_round::none));    // fin_sent + in_round
    CHECK(five_rules_admit(conn_phase::closed, read_mode::active, false, false, close_round::close));    // closed + round=close
    // published phases are compared by order
    CHECK(static_cast<int>(conn_phase::open) < static_cast<int>(conn_phase::closing) &&
          static_cast<int>(conn_phase::closing) < static_cast<int>(conn_phase::closed));
}

// ------------------------------------------------------------------ 2
static void test_cells() {
    std::puts("2. generated cells against the shipped transition()");
    constexpr size_t n_events = sizeof(kEventRows) / sizeof(kEventRows[0]);
    constexpr size_t n_cells = sizeof(kCellRows) / sizeof(kCellRows[0]);
    std::set<std::tuple<int, int, int>> listed;
    for (size_t i = 0; i < n_cells; ++i) {
        const cell_row& c = kCellRows[i];
        const conn_step r = transition(c.from, c.event, c.g);
        CHECK(r.next == c.to && r.action == c.action && r.fx == c.fx);
        listed.insert(std::make_tuple(int(c.from), int(c.event), int(c.g)));
        // table properties of the shipped switch
        CHECK(phase_of(c.to) >= phase_of(c.from));                                       // forward only
        if (phase_of(c.from) == conn_phase::closed) CHECK(phase_of(c.to) == conn_phase::closed);   // terminal
        if (phase_of(c.from) == conn_phase::open && phase_of(c.to) == conn_phase::draining) CHECK(c.fx & conn_fx::measure);
        if (!conn_sm::carries_stamp(c.from) && conn_sm::carries_stamp(c.to)) CHECK(c.fx & conn_fx::stamp);
        if (c.fx & conn_fx::resume) CHECK(read_of(c.from) == read_mode::paused && read_of(c.to) != read_mode::paused);
    }
    // everything not listed is inert
    size_t inert = 0;
    for (size_t si = 0; si < kStates; ++si) for (size_t ei = 0; ei < n_events; ++ei)
        for (int gi = 0; gi < kEventRows[ei].guard_count; ++gi) {
            const guard g = kEventRows[ei].guards[gi];
            if (listed.count(std::make_tuple(int(kStateRows[si].id), int(kEventRows[ei].event), int(g)))) continue;
            const conn_step r = transition(kStateRows[si].id, kEventRows[ei].event, g);
            CHECK(r.next == kStateRows[si].id && r.action == close_action::none && r.fx == 0);
            ++inert;
        }
    std::printf("  %zu events; %zu in-domain cells match; %zu out-of-domain cells inert\n", n_events, n_cells, inert);
}

// ------------------------------------------------------------------ 3
static conn_facts facts_of(const M::In& in) {
    conn_facts f;
    f.wbuf_empty = in.wbuf_empty; f.rbuf_empty = in.rbuf_empty;
    f.rbuf_readable = static_cast<size_t>(in.rbuf_readable); f.pending = in.pending;
    return f;
}

static void test_payloads() {
    std::puts("3. payloads through conn_sm::step()");
    size_t applied = 0;
    constexpr size_t n_cells = sizeof(kCellRows) / sizeof(kCellRows[0]);
    for (size_t i = 0; i < n_cells; ++i) {
        const cell_row& c = kCellRows[i];
        const bool needs_budget = c.event == conn_event::input_drained || c.event == conn_event::input_defer_failed;
        for (int64_t b : budgets_for(c.from, needs_budget)) {
            conn_state s = make(c.from, b);
            const int64_t old_stamp = s.stamp_ms;
            conn_inputs in; in.now = NOW; in.rbuf_readable = 999; in.dealt = 6;
            read_resume rr;
            conn_sm::step(s, c.event, c.g, in, &rr);
            ++applied;
            CHECK(s.id == c.to);
            // stamp: now if stamped, carried over if the same kind, zero where not carried
            if (conn_sm::carries_stamp(c.to)) {
                if (c.fx & conn_fx::stamp) CHECK(s.stamp_ms == NOW);
                else CHECK(s.stamp_ms == old_stamp && old_stamp != 0);
            } else CHECK(s.stamp_ms == 0);
            // the payload accessors read the stamp only where the state carries it
            CHECK(s.paused_at_ms() == (read_of(c.to) == read_mode::paused ? s.stamp_ms : 0));
            CHECK(s.close_started_at_ms() == (closing_phase(phase_of(c.to)) ? s.stamp_ms : 0));
            CHECK((s.paused_at_ms() != 0) == (read_of(c.to) == read_mode::paused));      // what the old pair of fields had to agree on
            CHECK(!(s.paused_at_ms() != 0 && s.close_started_at_ms() != 0));             // one slot suffices: they never coexist
            if (closing_phase(phase_of(c.to))) CHECK(s.close_started_at_ms() != 0);      // the close clock runs while parked
            // budget
            int64_t want = b;
            if ((c.fx & conn_fx::measure) && want < 0) want = 999;
            if ((c.fx & conn_fx::charge) && in.dealt > 0) want = std::max<int64_t>(0, want - in.dealt);
            CHECK(s.budget == want);
            // open has nothing to read; draining always has a budget
            if (phase_of(c.to) == conn_phase::open) CHECK(!s.close_input_bytes().has_value());
            if (phase_of(c.to) == conn_phase::draining) CHECK(s.close_input_bytes().has_value());
            CHECK(s.close_input_bytes().has_value() == (phase_of(c.to) != conn_phase::open && s.budget >= 0));
            // resume report
            CHECK(rr.resumed == ((c.fx & conn_fx::resume) != 0));
            if (rr.resumed) CHECK(rr.paused_for_ms == NOW - 7000);
        }
    }
    std::printf("  %zu cell applications (x budget variants) checked\n", applied);
}

// ------------------------------------------------------------------ 4
// The new machine, end to end, in the oracle's vocabulary.
static M::Out run_new(conn_state_id id, M::E e, const M::In& in) {
    conn_state s = make(id, in.budget);
    s.budget = in.budget;
    const conn_facts f = facts_of(in);
    close_action act = close_action::none;
    read_resume rr;
    switch (e) {
        case M::E::pause: conn_sm::on_pause(s, in.now); break;
        case M::E::resume: rr = conn_sm::on_resume(s, in.now); break;
        case M::E::eof: conn_sm::on_eof(s); break;
        case M::E::write_on: conn_sm::on_set_write(s, true); break;
        case M::E::write_off: conn_sm::on_set_write(s, false); break;
        case M::E::graceful: { auto st = conn_sm::on_graceful(s, in.now); act = st.action; rr = st.resume; break; }
        case M::E::request: { auto st = conn_sm::on_request(s, f, in.now); act = st.action; rr = st.resume; break; }
        case M::E::round_begin: conn_sm::on_round_begin(s); break;
        case M::E::round_end: conn_sm::on_round_end(s); break;
        case M::E::input_drained: act = conn_sm::on_input_drained(s, f, in.served, in.dealt); break;
        case M::E::input_defer_failed: act = conn_sm::on_input_defer_failed(s); break;
        case M::E::peer_fin: act = conn_sm::on_peer_fin(s, f); break;
        case M::E::fin_sent: conn_sm::on_fin_sent(s); break;
        case M::E::closed: conn_sm::on_closed(s); break;
        case M::E::COUNT: break;
    }
    M::Out o;
    o.next = axes_of_id(s.id);
    o.action = act; o.resumed = rr.resumed; o.paused_for = rr.paused_for_ms;
    o.budget_after = s.budget;
    o.next_carries = conn_sm::carries_stamp(s.id);
    o.stamp_after = s.stamp_ms;
    return o;
}

static void test_against_oracle() {
    std::puts("4. the whole machine against the five-axis oracle");
    long compared = 0;
    auto same = [&](const M::Axes& a, M::E e, const M::In& in, conn_state_id id) {
        const M::Out want = M::run<M::SpecOracle>(a, e, in);
        const M::Out got = run_new(id, e, in);
        ++compared;
        const bool ok = want.next == got.next && want.action == got.action && want.resumed == got.resumed &&
                        want.paused_for == got.paused_for && want.budget_after == got.budget_after &&
                        (!want.next_carries || want.stamp_after == got.stamp_after);
        if (!ok) std::printf("  oracle disagrees: %s from %s\n", M::ename(e), M::state_name(a).c_str());
        CHECK(ok);
    };
    for (size_t si = 0; si < kStates; ++si) {
        const conn_state_id id = kStateRows[si].id;
        const M::Axes a = axes_of_id(id);
        for (int ei = 0; ei < M::kEvents; ++ei) {
            const M::E e = static_cast<M::E>(ei);
            if (!M::in_domain(e, a)) continue;
            if (e == M::E::input_drained) {
                for (int served : {0, 1, 3}) for (int64_t dealt : {int64_t(0), int64_t(1), int64_t(6), int64_t(9000)})
                for (bool rbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(1), size_t(7)})
                for (int64_t budget : {int64_t(0), int64_t(1), int64_t(5), int64_t(500)}) {
                    M::In in; in.budget = budget; in.served = served; in.dealt = dealt;
                    in.rbuf_empty = rbuf_empty; in.rbuf_readable = rbuf_empty ? 0 : 10; in.pending = pending;
                    same(a, e, in, id);
                }
            } else if (e == M::E::peer_fin) {
                for (bool wbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(1), size_t(7)})
                for (int64_t budget : M::budget_variants(a, e)) {
                    M::In in; in.budget = budget; in.wbuf_empty = wbuf_empty; in.pending = pending;
                    same(a, e, in, id);
                }
            } else if (e == M::E::request) {
                for (bool wbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(7)})
                for (bool rbuf_empty : {false, true}) for (int64_t budget : M::budget_variants(a, e)) {
                    M::In in; in.budget = budget; in.wbuf_empty = wbuf_empty; in.pending = pending;
                    in.rbuf_empty = rbuf_empty; in.rbuf_readable = rbuf_empty ? 0 : 999;
                    same(a, e, in, id);
                }
            } else {
                for (int64_t budget : M::budget_variants(a, e)) {
                    M::In in; in.budget = budget; in.now = NOW;
                    same(a, e, in, id);
                }
            }
        }
    }
    std::printf("  %ld (state, event, facts) combinations agree: next state, action, resume, stamp, budget\n", compared);
}

// ------------------------------------------------------------------ 5
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
        f.closing.store(conn_sm::is_closing(s)); f.fin_sent = s.phase() == conn_phase::fin_sent;
        f.is_closed.store(s.phase() == conn_phase::closed);
        f.deferred_round = static_cast<L::close_fsm::deferred_kind>(static_cast<int>(s.round()));
        f.close_input_bytes = s.budget; f.draining_input = s.in_round();
        f.close_started_at_ms = s.close_started_at_ms();
        paused = s.read() == read_mode::paused; peer = s.read() == read_mode::eof; armed = s.write_armed();
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
    return conn_sm::is_closing(n) == lw.f.closing.load() && (n.phase() == conn_phase::fin_sent) == lw.f.fin_sent &&
           (n.phase() == conn_phase::closed) == lw.f.is_closed.load() &&
           static_cast<int>(n.round()) == static_cast<int>(lw.f.deferred_round) &&
           n.budget == expect_cib(cib_before, lw.f.close_input_bytes) &&
           n.in_round() == lw.f.draining_input &&
           (n.close_started_at_ms() != 0) == (lw.f.close_started_at_ms != 0);
}

static std::vector<conn_facts> all_facts() {
    std::vector<conn_facts> v;
    for (int w = 0; w < 2; ++w) for (int r = 0; r < 2; ++r) for (size_t p : {size_t(0), size_t(1)}) {
        conn_facts x; x.wbuf_empty = w; x.rbuf_empty = r; x.rbuf_readable = r ? 0 : 10; x.pending = p; v.push_back(x);
    }
    return v;
}

static void test_against_previous() {
    std::puts("5. close decisions against the previous implementation");
    size_t compared = 0;
    for (size_t si = 0; si < kStates; ++si) {
        const conn_state_id id = kStateRows[si].id;
        for (int64_t b : budgets_for(id, false)) for (const auto& cf : all_facts()) {
            const conn_state s0 = make(id, b);
            { conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
              auto st = conn_sm::on_graceful(s, NOW); auto la = lw.f.on_graceful();
              CHECK(st.action == to_new(la)); CHECK(same_close_state(s, lw, s0.budget));
              CHECK(st.resume.resumed == (la != L::close_action::none && lw.resume())); ++compared; }
            { conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
              auto st = conn_sm::on_request(s, cf, NOW); auto la = lw.f.on_request();
              CHECK(st.action == to_new(la)); CHECK(same_close_state(s, lw, s0.budget));
              CHECK(st.resume.resumed == (la != L::close_action::none && lw.resume())); ++compared; }
            const bool measured = phase_of(id) != conn_phase::open && b >= 0;
            if (conn_sm::is_done(s0) || measured) {
                for (int served : {0, 1, 3}) for (int64_t dealt : {int64_t(0), int64_t(1), int64_t(6), int64_t(9000)}) {
                    conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
                    auto a = conn_sm::on_input_drained(s, cf, served, dealt); auto la = lw.f.on_input_drained(served, dealt);
                    CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.budget)); ++compared;
                }
            }
            if (measured) { conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
                auto a = conn_sm::on_input_defer_failed(s); auto la = lw.f.on_input_defer_failed();
                CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.budget)); ++compared; }
            if (read_of(id) == read_mode::eof) { conn_state s = s0; LegacyWorld lw; lw.load(s0); lw.facts(cf);
                auto a = conn_sm::on_peer_fin(s, cf); auto la = lw.f.on_peer_fin();
                CHECK(a == to_new(la)); CHECK(same_close_state(s, lw, s0.budget)); ++compared; }
        }
    }
    for (auto r : {drain_outcome::drained, drain_outcome::stalled, drain_outcome::dead, drain_outcome::error}) {
        L::close_fsm lf; CHECK(conn_sm::on_drain_result(r) == to_new(lf.on_drain_result(static_cast<L::drain_outcome>(static_cast<int>(r)))));
    }
    // the read/write flags against the booleans they replaced
    for (size_t si = 0; si < kStates; ++si) {
        const conn_state s0 = make(kStateRows[si].id);
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          const bool nr = conn_sm::on_pause(s, NOW), lr = lw.pause(); CHECK(nr == lr);
          if (s0.phase() != conn_phase::closed) { CHECK((s.read() == read_mode::paused) == lw.paused); }
          ++compared; }
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          CHECK(conn_sm::on_resume(s, NOW).resumed == lw.resume()); ++compared; }
        { conn_state s = s0; LegacyWorld lw; lw.load(s0);
          for (bool a : {true, false, true}) { CHECK(conn_sm::on_set_write(s, a) == lw.set_write(a)); CHECK(s.write_armed() == lw.armed); } ++compared; }
        { const uint32_t legacy_interest = (s0.read() == read_mode::active ? uint32_t(EPOLLIN) : 0u) | (s0.write_armed() ? uint32_t(EPOLLOUT) : 0u);
          CHECK(conn_sm::interest(s0) == legacy_interest); }
    }
    std::printf("  %zu decisions compared, all agree (modulo test 7)\n", compared);
}

// ------------------------------------------------------------------ 6
static void test_random_walk(unsigned seed, int steps) {
    std::mt19937 rng(seed);
    auto rnd = [&](int n) { return int(rng() % unsigned(n)); };
    conn_state s; LegacyWorld lw;
    long applied = 0, resets = 0; long phase_visits[5] = {};
    auto compare = [&](int64_t cib_before, const char* what) {
        const bool closed = s.phase() == conn_phase::closed;
        CHECK(closed == lw.f.is_closed.load());
        if (!closed) {
            CHECK(conn_sm::is_closing(s) == lw.f.closing.load());
            CHECK((s.phase() == conn_phase::fin_sent) == lw.f.fin_sent);
        }
        CHECK(static_cast<int>(s.round()) == static_cast<int>(lw.f.deferred_round));
        CHECK(s.budget == expect_cib(cib_before, lw.f.close_input_bytes));
        if (!closed) {
            CHECK(s.in_round() == lw.f.draining_input);
            CHECK((s.read() == read_mode::paused) == lw.paused);
            CHECK((s.read() == read_mode::eof) == lw.peer);
            CHECK(s.write_armed() == lw.armed);
        }
        CHECK((s.phase() == conn_phase::draining) == (!lw.f.closing.load() && !lw.f.is_closed.load() && lw.f.close_input_bytes >= 0));
        (void)what;
        ++phase_visits[int(s.phase())];
    };
    auto peer_fin_effects = [&](const conn_facts& cf) {
        if (s.read() != read_mode::eof || s.phase() == conn_phase::closed) return;
        if (!cf.wbuf_empty) { conn_sm::on_set_write(s, true); lw.set_write(true); return; }
        const int64_t before = s.budget;
        auto a = conn_sm::on_peer_fin(s, cf); auto la = lw.f.on_peer_fin();
        CHECK(a == to_new(la));
        if (a == close_action::send_fin) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; }
        if (a == close_action::close_graceful) { conn_sm::on_closed(s); lw.f.is_closed.store(true); }
        compare(before, "peer_fin");
    };
    for (int i = 0; i < steps; ++i) {
        conn_facts cf; cf.wbuf_empty = rnd(3) != 0; cf.rbuf_empty = rnd(2) == 0; cf.rbuf_readable = cf.rbuf_empty ? 0 : 10; cf.pending = rnd(2) ? 0 : 5;
        lw.facts(cf);
        const int64_t before = s.budget;
        const int ev = rnd(12);
        switch (ev) {
        case 0: { CHECK(conn_sm::on_pause(s, NOW + i) == lw.pause()); compare(before, "pause"); break; }
        case 1: { const bool was_closed = s.phase() == conn_phase::closed;
                  const bool nr = conn_sm::on_resume(s, NOW + i).resumed, lr = lw.resume();
                  if (!was_closed) CHECK(nr == lr);
                  compare(before, "resume"); break; }
        case 2: { if (s.read() == read_mode::active && s.phase() != conn_phase::closed) { conn_sm::on_eof(s); lw.peer = true; compare(before, "eof"); } break; }
        case 3: { if (s.phase() != conn_phase::closed) { bool a = rnd(2); CHECK(conn_sm::on_set_write(s, a) == lw.set_write(a)); compare(before, "set_write"); } break; }
        case 4: { auto st = conn_sm::on_graceful(s, NOW + i); auto la = lw.f.on_graceful();
                  CHECK(st.action == to_new(la)); bool lres = la != L::close_action::none && lw.resume();
                  CHECK(st.resume.resumed == lres); compare(before, "graceful"); break; }
        case 5: case 6: {
            auto st = conn_sm::on_request(s, cf, NOW + i); auto la = lw.f.on_request();
            CHECK(st.action == to_new(la)); bool lres = la != L::close_action::none && lw.resume();
            CHECK(st.resume.resumed == lres); compare(before, "request");
            if (st.action == close_action::none) break;
            conn_sm::on_round_begin(s); lw.f.draining_input = true; compare(s.budget, "round_begin");
            const int inner = rnd(5);
            if (inner == 1 && s.read() == read_mode::active) { conn_sm::on_eof(s); lw.peer = true; peer_fin_effects(cf); }
            if (inner == 2 && s.phase() != conn_phase::closed) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; }
            conn_sm::on_round_end(s); lw.f.draining_input = false;
            if (s.phase() == conn_phase::closed || s.phase() == conn_phase::fin_sent) { compare(s.budget, "round_end"); break; }
            const int served = rnd(4);
            const int64_t dealt = served > 0 ? int64_t(rnd(int(std::min<int64_t>(s.budget, 40)) + 1)) : 0;
            const int64_t cb = s.budget;
            close_action a = conn_sm::on_input_drained(s, cf, served, dealt); auto la2 = lw.f.on_input_drained(served, dealt);
            CHECK(a == to_new(la2)); compare(cb, "input_drained");
            if (a == close_action::defer_drain_input && rnd(3) == 0) {
                const int64_t cb2 = s.budget;
                a = conn_sm::on_input_defer_failed(s); auto la3 = lw.f.on_input_defer_failed();
                CHECK(a == to_new(la3)); compare(cb2, "defer_failed");
            }
            if (a == close_action::drain_wbuf || (a == close_action::defer_park && rnd(3) == 0)) {
                auto g = conn_sm::on_graceful(s, NOW + i); auto lg = lw.f.on_graceful();
                CHECK(g.action == to_new(lg)); bool lr2 = lg != L::close_action::none && lw.resume();
                CHECK(g.resume.resumed == lr2); compare(s.budget, "park");
            }
            break; }
        case 7: { peer_fin_effects(cf); break; }
        case 8: {
            if (s.phase() == conn_phase::closed || s.phase() == conn_phase::fin_sent) break;
            const drain_outcome r[] = {drain_outcome::drained, drain_outcome::stalled, drain_outcome::dead, drain_outcome::error};
            const close_action a = conn_sm::on_drain_result(r[rnd(4)]);
            if (a == close_action::close_abort) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; compare(before, "abort"); break; }
            if (a == close_action::none) break;
            if (s.read() == read_mode::eof) { peer_fin_effects(cf); break; }
            conn_sm::on_set_write(s, false); lw.set_write(false);
            if (s.phase() == conn_phase::closing) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; }
            compare(before, "handle_write"); break; }
        case 9: { if (rnd(6) == 0) { conn_sm::on_closed(s); lw.f.is_closed.store(true); lw.armed = false; compare(before, "closed"); } break; }
        default: { if (s.phase() == conn_phase::closing && cf.wbuf_empty) { conn_sm::on_fin_sent(s); lw.f.fin_sent = true; compare(before, "fin"); } break; }
        }
        ++applied;
        if (s.phase() == conn_phase::closed && rnd(4) == 0) { s = conn_state(); lw.f.closing.store(false); lw.f.fin_sent = false; lw.f.is_closed.store(false);
            lw.f.deferred_round = L::close_fsm::deferred_kind::none; lw.f.close_input_bytes = -1; lw.f.draining_input = false; lw.f.close_started_at_ms = 0;
            lw.paused = lw.peer = lw.armed = false; ++resets; }
    }
    if (seed == 1) std::printf("  seed 1: %ld events over %ld connection lifetimes; phase visits open=%ld draining=%ld closing=%ld fin_sent=%ld closed=%ld\n",
                               applied, resets + 1, phase_visits[0], phase_visits[1], phase_visits[2], phase_visits[3], phase_visits[4]);
}

// ------------------------------------------------------------------ 7
static void test_exhausted_budget() {
    std::puts("7. exhausted input budget (the deliberate difference from the previous implementation)");
    conn_facts cf; cf.rbuf_empty = false; cf.rbuf_readable = 700; cf.pending = 0; cf.wbuf_empty = true;
    conn_state s;
    conn_sm::on_request(s, cf, NOW);
    CHECK(s.close_input_bytes().value_or(-1) == 700);
    cf.rbuf_readable = 10; cf.rbuf_empty = false;
    auto a = conn_sm::on_input_drained(s, cf, 5, 1000);
    CHECK(s.close_input_bytes().value_or(-1) == 0);            // exhausted, not "unmeasured"
    CHECK(a != close_action::defer_drain_input);
    conn_sm::on_request(s, cf, NOW + 5);
    CHECK(s.close_input_bytes().value_or(-1) == 0);            // no fresh budget for a second close request
    LegacyWorld lw; lw.facts(conn_facts{true, false, 700, 0});
    lw.f.on_request(); lw.facts(conn_facts{true, false, 10, 0});
    lw.f.on_input_drained(5, 1000);
    const int64_t exhausted = lw.f.close_input_bytes;
    lw.f.on_request();
    std::printf("  previous: budget after the round = %lld, after a second close request = %lld (re-measured from rbuf)\n",
                (long long)exhausted, (long long)lw.f.close_input_bytes);
    std::printf("  now:      budget after the round = 0, after a second close request = %lld\n", (long long)s.budget);
    CHECK(exhausted < 0);
    CHECK(lw.f.close_input_bytes == 10);
}

// ------------------------------------------------------------------ 8
static void test_machine_class() {
    std::puts("8. conn_machine: publication and the cross-thread channel");
    ring_buffer wb, rb; wb.init(64); rb.init(64); std::atomic<size_t> pending{0};
    conn_machine m; m.attach(&wb, &rb, &pending);
    CHECK(m.published_phase() == conn_phase::open && !m.closing() && !m.closed() && !m.refuses_sends());
    CHECK(m.on_pause(NOW)); CHECK(m.read_paused()); CHECK(m.state().paused_at_ms() == NOW);
    CHECK(m.interest() == 0);
    CHECK(m.on_set_write(true)); CHECK(m.interest() == EPOLLOUT);
    uint8_t b[10] = {}; rb.write(b, 10); wb.write(b, 4); pending.store(3);
    auto st = m.on_request(NOW + 10);
    CHECK(st.action == close_action::drain_input); CHECK(st.resume.resumed); CHECK(st.resume.paused_for_ms == 10);
    CHECK(!m.read_paused()); CHECK(m.state().phase() == conn_phase::draining); CHECK(m.state().close_input_bytes().value_or(-1) == 10);
    CHECK(m.state().paused_at_ms() == 0);
    CHECK(m.published_phase() == conn_phase::draining);
    CHECK(!m.refuses_sends());
    CHECK(m.interest() == (EPOLLIN | EPOLLOUT));
    m.on_round_begin(); CHECK(m.state().in_round()); CHECK(!m.on_pause(NOW + 11)); CHECK(!m.backpressure_applies()); m.on_round_end();
    CHECK(m.backpressure_applies());
    auto g = m.on_graceful(NOW + 20);
    CHECK(g.action == close_action::drain_wbuf); CHECK(!g.resume.resumed);
    CHECK(m.closing() && m.refuses_sends() && !m.closed() && !m.fin_sent());
    CHECK(m.close_started_at_ms() == NOW + 20);
    CHECK(!m.on_pause(NOW + 21));
    wb.clear(); m.on_fin_sent(); CHECK(m.fin_sent() && m.closing());
    CHECK(m.close_started_at_ms() == NOW + 20);                // carried through the FIN
    CHECK(m.on_closed()); CHECK(!m.on_closed()); CHECK(m.closed() && m.refuses_sends());
    CHECK(!m.write_armed());
    CHECK(!m.on_set_write(true));
    CHECK(m.close_started_at_ms() == 0);                       // a closed connection claims no clock
    conn_machine n; CHECK(!n.close_request_pending());
    n.note_close_request(); CHECK(n.close_request_pending());
    CHECK(!n.take_close_wanted()); n.post_close_wanted(); CHECK(n.take_close_wanted()); CHECK(!n.take_close_wanted());
    conn_machine r; r.attach(&wb, &rb, &pending); r.on_rejected(); CHECK(r.closed());
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

int main() {
    test_states();
    test_cells();
    test_payloads();
    test_against_oracle();
    test_against_previous();
    std::puts("6. lock-step random walks against the previous implementation");
    for (unsigned seed = 1; seed <= 40; ++seed) test_random_walk(seed, 20000);
    std::printf("  40 walks x 20000 events, all agree\n");
    test_exhausted_budget();
    test_machine_class();
    std::printf("\n%ld checks: %s (%d failures)\n", checks, fails ? "FAILED" : "ALL PASSED", fails);
    return fails != 0;
}
