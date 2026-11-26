// tools/gen_conn_fsm.cpp -- the source of truth for the connection state machine.
//
//   gen_conn_fsm [--header H] [--cases C] [--doc D] [--fragment F] [--check]
//
// It derives the machine's table by running the oracle (tools/conn_fsm_spec.h)
// on every legal state, checks the derivation, and emits, from the same data:
//   --header   rewrites the region between the BEGIN/END GENERATED markers in
//              tcp-server-mt.h: the closed state enum, its accessors, the event
//              and guard enums and the inline-switch transition function.
//   --cases    verify/conn_fsm_cases.inc: every legal state and every in-domain
//              (state, event, guard) cell, for the exhaustive test.
//   --doc      docs/conn-fsm.md.
//   --fragment just the generated region (used to diff against the old machine
//              before it is deleted).
//   --check    write nothing; exit non-zero if any output is out of date.
//
// Nothing here is hand-copied into the header: change a rule in
// conn_fsm_spec.h, run this, and all three outputs follow.
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>

#include "conn_fsm_model.h"

using namespace model;

static const char* kBegin = "// ===== BEGIN GENERATED (tools/gen_conn_fsm.cpp; do not edit by hand) =====";
static const char* kEnd = "// ===== END GENERATED =====";

static int errors_seen = 0;
static void fail(const std::string& m) { std::fprintf(stderr, "generator: FAIL: %s\n", m.c_str()); ++errors_seen; }

// ------------------------------------------------------------------ checks
struct Table {
    Derived d;
    std::map<std::tuple<int, int, int>, const Cell*> by_key;   // (state, event, guard) -> cell
    const Cell* find(int s, E e, G g) const {
        auto it = by_key.find(std::make_tuple(s, static_cast<int>(e), static_cast<int>(g)));
        return it == by_key.end() ? nullptr : it->second;
    }
    int idx(const Axes& a) const {
        for (size_t i = 0; i < d.states.size(); ++i) if (d.states[i] == a) return static_cast<int>(i);
        return -1;
    }
};

static int64_t predict_budget(const Cell& c, const In& in) {
    int64_t b = in.budget;
    if ((c.fx & fx::measure) && b < 0) b = in.rbuf_readable;
    if ((c.fx & fx::charge) && in.dealt > 0) b = std::max<int64_t>(0, b - in.dealt);
    return b;
}

static void check_structure(Table& t) {
    // 1. exactly the 77 states, in the counts the design fixes
    int per[5] = {0, 0, 0, 0, 0};
    for (const auto& a : t.d.states) ++per[static_cast<int>(a.phase)];
    const int want[5] = {8, 24, 20, 20, 5};
    for (int i = 0; i < 5; ++i)
        if (per[i] != want[i]) fail(std::string("phase ") + phase_name(static_cast<conn_phase>(i)) + " has " + std::to_string(per[i]) + " states, expected " + std::to_string(want[i]));
    if (t.d.states.size() != 77) fail("expected 77 states, got " + std::to_string(t.d.states.size()));
    // names are unique
    std::set<std::string> names;
    for (const auto& a : t.d.states) if (!names.insert(state_name(a)).second) fail("duplicate state name " + state_name(a));

    // 2. the five discrete rules admit exactly what the old machine's full
    //    invariant admits once payloads are filled in consistently
    int agree = 0;
    for (int p = 0; p < 5; ++p) for (int r = 0; r < 3; ++r) for (int ir = 0; ir < 2; ++ir)
    for (int rn = 0; rn < 3; ++rn) for (int w = 0; w < 2; ++w) {
        Axes a; a.phase = static_cast<conn_phase>(p); a.read = static_cast<read_mode>(r);
        a.in_round = ir; a.round = static_cast<close_round>(rn); a.write = w;
        bool old_admits = false;
        for (int64_t b : {int64_t(-1), int64_t(500)})
            old_admits = old_admits || conn_fsm_spec::sm::valid(make_state<conn_fsm_spec::state>(a, b));
        if (old_admits != legal(a)) fail("five-rule legality disagrees with the old invariant for " + state_name(a));
        else ++agree;
    }
    std::printf("generator: %zu legal states of 180 combinations; five rules agree with the old invariant on all %d\n", t.d.states.size(), agree);

    // 3. derivation errors (closure, payload discipline)
    for (const auto& e : t.d.errors) fail(e);

    // 4. forward-only phases, closed is terminal, draining is entered with a budget
    for (const auto& c : t.d.cells) {
        const Axes& from = t.d.states[c.from]; const Axes& to = t.d.states[c.to];
        const std::string where = std::string(ename(c.e)) + "/" + gname(c.g) + " from " + state_name(from);
        if (to.phase < from.phase) fail(where + ": phase moves backwards");
        if (from.phase == conn_phase::closed && to.phase != conn_phase::closed) fail(where + ": closed is not terminal");
        if (from.phase == conn_phase::open && to.phase == conn_phase::draining && !(c.fx & fx::measure))
            fail(where + ": enters draining without measuring the budget");
        if (!carries_stamp(from) && carries_stamp(to) && !(c.fx & fx::stamp))
            fail(where + ": enters a stamp-carrying state without stamping it");
        if ((c.fx & fx::charge) && from.phase == conn_phase::open) fail(where + ": charges a budget that open does not have");
    }
}

// The guard reduction must be lossless: whatever the facts, the oracle's
// answer is the answer of the cell the guard selects.
static void check_guards(const Table& t) {
    long runs = 0;
    auto compare = [&](size_t si, E e, G g, const In& in) {
        const Axes& a = t.d.states[si];
        const Cell* c = t.find(static_cast<int>(si), e, g);
        const std::string where = std::string(ename(e)) + "/" + gname(g) + " from " + state_name(a);
        if (!c) { fail(where + ": no cell for the guard the classifier chose"); return; }
        const Out o = run<SpecOracle>(a, e, in);
        ++runs;
        if (t.idx(o.next) != c->to) fail(where + ": next state differs from the guard's cell");
        if (o.action != c->action) fail(where + ": action differs from the guard's cell");
        if (o.resumed != ((c->fx & fx::resume) != 0)) fail(where + ": resume differs from the guard's cell");
        if (o.budget_after != predict_budget(*c, in)) fail(where + ": budget payload differs from the guard's cell");
        if (carries_stamp(o.next)) {
            const bool src_c = a.phase == conn_phase::closing || a.phase == conn_phase::fin_sent;
            const int64_t want = (c->fx & fx::stamp) ? in.now : (o.next.read == read_mode::paused ? (a.read == read_mode::paused ? kPausedStamp : -1) : (src_c ? kCloseStamp : -1));
            if (o.stamp_after != want) fail(where + ": stamp payload differs from the guard's cell");
        }
    };
    for (size_t si = 0; si < t.d.states.size(); ++si) {
        const Axes& a = t.d.states[si];
        if (a.phase != conn_phase::open)
            for (int served : {0, 1, 3}) for (int64_t dealt : {int64_t(0), int64_t(1), int64_t(6), int64_t(9000)})
            for (bool rbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(1), size_t(7)})
            for (int64_t budget : {int64_t(0), int64_t(1), int64_t(5), int64_t(500)}) {
                In in; in.budget = budget; in.served = served; in.dealt = dealt;
                in.rbuf_empty = rbuf_empty; in.rbuf_readable = rbuf_empty ? 0 : 10; in.pending = pending;
                const int64_t after = (served > 0 && dealt > 0) ? std::max<int64_t>(0, budget - dealt) : budget;
                compare(si, E::input_drained, input_guard(served > 0, !rbuf_empty, after > 0, pending > 0), in);
            }
        if (in_domain(E::peer_fin, a))
            for (bool wbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(1), size_t(7)})
            for (bool rbuf_empty : {false, true}) for (int64_t budget : budget_variants(a, E::peer_fin)) {
                In in; in.budget = budget; in.wbuf_empty = wbuf_empty; in.pending = pending; in.rbuf_empty = rbuf_empty;
                compare(si, E::peer_fin, peer_guard(wbuf_empty, pending > 0), in);
            }
        // request reads facts but decides on none of them
        for (bool wbuf_empty : {false, true}) for (size_t pending : {size_t(0), size_t(7)})
        for (bool rbuf_empty : {false, true}) for (int64_t budget : budget_variants(a, E::request)) {
            In in; in.budget = budget; in.wbuf_empty = wbuf_empty; in.pending = pending;
            in.rbuf_empty = rbuf_empty; in.rbuf_readable = rbuf_empty ? 0 : kRbufReadable;
            compare(si, E::request, G::none, in);
        }
    }
    std::printf("generator: guard reduction checked against the oracle over %ld fact combinations\n", runs);
}

// ------------------------------------------------------------------ emit helpers
static std::string S(const Table& t, int i) { return "S::" + state_name(t.d.states[i]); }

static std::string wrap_cases(const Table& t, const std::vector<int>& ids, const char* indent) {
    // cpplint's 80-column rule counts the indent, so the budget for the labels
    // is the remainder. A label list ends in a space per item; those are
    // stripped at the break or they become trailing whitespace.
    const size_t width = 80 - std::string(indent).size();
    std::string out, line;
    for (int i : ids) {
        std::string item = "case " + S(t, i) + ": ";
        if (!line.empty() && line.size() + item.size() > width) {
            while (!line.empty() && line.back() == ' ') line.pop_back();
            out += indent + line + "\n";
            line.clear();
        }
        line += item;
    }
    while (!line.empty() && line.back() == ' ') line.pop_back();
    if (!line.empty()) out += indent + line;
    return out;
}

template <class T, class F>
static void emit_accessor(std::ostringstream& o, const Table& t, const char* sig, T dflt, F value,
                          const std::vector<T>& order, const char* (*text)(T)) {
    o << "constexpr " << sig << "(conn_state_id s) noexcept {\n    using S = conn_state_id;\n    switch (s) {\n";
    for (T v : order) {
        if (v == dflt) continue;
        std::vector<int> ids;
        for (size_t i = 0; i < t.d.states.size(); ++i) if (value(t.d.states[i]) == v) ids.push_back(static_cast<int>(i));
        if (ids.empty()) continue;
        o << wrap_cases(t, ids, "    ") << "\n        return " << text(v) << ";\n";
    }
    o << "    default: break;\n    }\n    return " << text(dflt) << ";\n}\n";
}
static const char* t_phase(conn_phase p) { static std::string n[5]; int i = static_cast<int>(p); n[i] = std::string("conn_phase::") + phase_name(p); return n[i].c_str(); }
static const char* t_read(read_mode r) { static std::string n[3]; int i = static_cast<int>(r); n[i] = std::string("read_mode::") + read_name(r); return n[i].c_str(); }
static const char* t_round(close_round r) { static std::string n[3]; int i = static_cast<int>(r); n[i] = std::string("close_round::") + round_name(r); return n[i].c_str(); }
static const char* t_bool(bool b) { return b ? "true" : "false"; }

// ------------------------------------------------------------------ the header region
static std::string emit_region(const Table& t) {
    std::ostringstream o;
    o << kBegin << "\n";
    o << "// Source of truth: tools/conn_fsm_spec.h (the rules) -> tools/gen_conn_fsm.cpp\n"
         "// (this text, verify/conn_fsm_cases.inc, docs/conn-fsm.md).\n"
         "// Regenerate; do not edit.\n//\n"
         "// A state is one value of conn_state_id: a closed enum of the " << t.d.states.size() << " legal\n"
         "// combinations of (phase, read, in_round, round, write_armed). A combination\n"
         "// the five rules forbid has no enumerator, so it cannot be named, let alone\n"
         "// reached. The rules, from the 180 combinations of the five axes:\n"
         "//   - round == close needs phase != open;\n"
         "//   - round == peer_closed needs read == eof;\n"
         "//   - read == paused needs phase open/draining and no serving round;\n"
         "//   - closed has no write interest, no serving round and no pause;\n"
         "//   - a serving round needs phase != open.\n"
         "// Per phase: open 8, draining 24, closing 20, fin_sent 20, closed 5.\n//\n"
         "// <phase>_<read:act|pau|eof>[_<flags>]; flags in this order:\n"
         "//   R serving round on the stack, C round=close, P round=peer_closed,\n"
         "//   W write interest armed.\n"
         "// Ids are ordered by (phase, read, in_round, round, write).\n";
    o << "enum class conn_state_id : uint8_t {\n";
    int prev_phase = -1;
    for (size_t i = 0; i < t.d.states.size(); ++i) {
        const Axes& a = t.d.states[i];
        if (static_cast<int>(a.phase) != prev_phase) {
            prev_phase = static_cast<int>(a.phase);
            int n = 0; for (auto& b : t.d.states) n += b.phase == a.phase;
            o << "    // " << phase_name(a.phase) << " (" << n << ")\n";
        }
        o << "    " << state_name(a) << " = " << i << ",\n";
    }
    o << "};\ninline constexpr int k_conn_state_count = " << t.d.states.size() << ";\n\n";

    std::vector<conn_phase> ph = {conn_phase::open, conn_phase::draining, conn_phase::closing, conn_phase::fin_sent, conn_phase::closed};
    std::vector<read_mode> rd = {read_mode::active, read_mode::paused, read_mode::eof};
    std::vector<close_round> rn = {close_round::none, close_round::close, close_round::peer_closed};
    std::vector<bool> bl = {false, true};
    emit_accessor<conn_phase>(o, t, "conn_phase phase_of", conn_phase::open, [](const Axes& a) { return a.phase; }, ph, t_phase);
    o << "\n";
    emit_accessor<read_mode>(o, t, "read_mode read_of", read_mode::active, [](const Axes& a) { return a.read; }, rd, t_read);
    o << "\n";
    emit_accessor<close_round>(o, t, "close_round round_of", close_round::none, [](const Axes& a) { return a.round; }, rn, t_round);
    o << "\n";
    emit_accessor<bool>(o, t, "bool write_armed_of", false, [](const Axes& a) { return a.write; }, bl, t_bool);
    o << "\n";
    emit_accessor<bool>(o, t, "bool in_round_of", false, [](const Axes& a) { return a.in_round; }, bl, t_bool);
    o << "\n";
    o << "constexpr const char* name_of(conn_state_id s) noexcept {\n    switch (s) {\n";
    for (size_t i = 0; i < t.d.states.size(); ++i)
        o << "    case conn_state_id::" << state_name(t.d.states[i]) << ": return \"" << state_name(t.d.states[i]) << "\";\n";
    o << "    }\n    return \"?\";\n}\n\n";

    o << "// Events. Each is a function of the state and, where the decision reads the\n"
         "// connection's buffers, of one guard.\n"
         "enum class conn_event : uint8_t {\n";
    for (int i = 0; i < kEvents; ++i) o << "    " << ename(static_cast<E>(i)) << ",\n";
    o << "};\n\n"
         "// The facts an event branches on, reduced to the classes its decision\n"
         "// distinguishes. The reduction is lossless (the generator checks the oracle\n"
         "// against it over every combination of the facts).\n"
         "//   input_drained (a serving round finished):\n"
         "//     more       served > 0, rbuf still holds input, budget left\n"
         "//     served     served > 0 otherwise\n"
         "//     idle_owed  served == 0, replies are owed (pending > 0)\n"
         "//     idle_clear served == 0, nothing owed\n"
         "//   peer_fin (the peer's FIN, wbuf permitting):\n"
         "//     wbuf_busy  wbuf holds bytes: not our turn yet\n"
         "//     owed       wbuf empty, replies still reserved\n"
         "//     clear      wbuf empty, nothing owed\n"
         "enum class guard : uint8_t {\n    none,\n    more, served, idle_owed, idle_clear,\n    wbuf_busy, owed, clear,\n};\n\n"
         "inline constexpr guard input_guard(bool served, bool rbuf_nonempty,\n"
         "                                   bool budget_left, bool pending) noexcept {\n"
         "    if (served) {\n"
         "        const bool more = rbuf_nonempty && budget_left;\n"
         "        return more ? guard::more : guard::served;\n"
         "    }\n"
         "    return pending ? guard::idle_owed : guard::idle_clear;\n}\n"
         "inline constexpr guard peer_guard(bool wbuf_empty, bool pending) noexcept {\n"
         "    if (!wbuf_empty) return guard::wbuf_busy;\n"
         "    return pending ? guard::owed : guard::clear;\n}\n\n"
         "// Payload effects of a transition, applied by the wrapper that owns the\n"
         "// payloads (the table decides *whether*, the payload decides *what*).\n"
         "namespace conn_fx {\n"
         "constexpr uint8_t stamp = 1;    // the state's stamp := now\n"
         "constexpr uint8_t measure = 2;  // unmeasured budget := rbuf bytes\n"
         "constexpr uint8_t charge = 4;   // budget := max(0, budget - dealt)\n"
         "constexpr uint8_t resume = 8;   // reads were resumed: report it to the reactor\n"
         "}  // namespace conn_fx\n\n"
         "struct conn_step {\n    conn_state_id next;\n    close_action  action;\n    uint8_t       fx;\n};\n\n";

    o << "// (state, event, guard) -> (next, action, effects). Cells not listed leave the\n"
         "// state alone and do nothing; an event issued outside its precondition (see\n"
         "// conn_machine) is such a cell. An inline switch, not a data array: every\n"
         "// translation unit includes this header.\n"
         "constexpr conn_step transition(conn_state_id s, conn_event e,\n"
         "                              guard g) noexcept {\n"
         "    using S = conn_state_id;\n    using A = close_action;\n    (void)g;\n    switch (e) {\n";
    for (int ei = 0; ei < kEvents; ++ei) {
        const E e = static_cast<E>(ei);
        o << "    case conn_event::" << ename(e) << ":\n";
        const bool guarded = guards_of(e).size() > 1;
        std::string ind = "        ";
        if (guarded) { o << "        switch (g) {\n"; }
        for (G g : guards_of(e)) {
            if (guarded) o << "        case guard::" << gname(g) << ":\n";
            const std::string i1 = guarded ? "            " : "        ";
            o << i1 << "switch (s) {\n";
            for (const auto& c : t.d.cells) {
                if (c.e != e || c.g != g) continue;
                if (c.to == c.from && c.action == close_action::none && c.fx == 0) continue;
                // One row per cell, on one line while it fits cpplint's 80
                // columns; otherwise the label and the step are split so the
                // generated region passes the repository's lint gate.
                const std::string ret = "return {" + S(t, c.to) + ", A::" +
                    action_name(c.action) + ", " + fx_expr(c.fx) + "};";
                const std::string row = "case " + S(t, c.from) + ": " + ret;
                if (i1.size() + row.size() <= 80) {
                    o << i1 << row << "\n";
                } else if (i1.size() + 4 + ret.size() <= 80) {
                    o << i1 << "case " << S(t, c.from) << ":\n"
                      << i1 << "    " << ret << "\n";
                } else {
                    // Even the step alone is too wide: break it after the
                    // action so the effects expression starts the next line.
                    o << i1 << "case " << S(t, c.from) << ":\n"
                      << i1 << "    return {" << S(t, c.to) << ", A::"
                      << action_name(c.action) << ",\n"
                      << i1 << "            " << fx_expr(c.fx) << "};\n";
                }
            }
            o << i1 << "default: break;\n" << i1 << "}\n";
            if (guarded) o << "            break;\n";
        }
        if (guarded) o << "        default: break;\n        }\n";
        o << "        break;\n";
    }
    o << "    }\n    return {s, A::none, 0};\n}\n";
    o << kEnd << "\n";
    return o.str();
}

// ------------------------------------------------------------------ test cases
static std::string emit_cases(const Table& t) {
    std::ostringstream o;
    o << "// GENERATED by tools/gen_conn_fsm.cpp from tools/conn_fsm_spec.h -- do not edit.\n"
         "// Included inside a function-free scope after tcp-server-mt.h with\n"
         "// `using namespace industrial;` in effect.\n\n"
         "struct state_row {\n    conn_state_id id;\n    conn_phase    phase;\n    read_mode     read;\n    bool          write_armed;\n    bool          in_round;\n    close_round   round;\n    const char*   name;\n};\n"
         "static const state_row kStateRows[] = {\n";
    for (size_t i = 0; i < t.d.states.size(); ++i) {
        const Axes& a = t.d.states[i];
        o << "    {conn_state_id::" << state_name(a) << ", conn_phase::" << phase_name(a.phase) << ", read_mode::" << read_name(a.read)
          << ", " << t_bool(a.write) << ", " << t_bool(a.in_round) << ", close_round::" << round_name(a.round) << ", \"" << state_name(a) << "\"},\n";
    }
    o << "};\n\n"
         "struct event_row {\n    conn_event  event;\n    const char* name;\n    int         guard_count;\n    guard       guards[4];\n};\n"
         "static const event_row kEventRows[] = {\n";
    for (int ei = 0; ei < kEvents; ++ei) {
        const E e = static_cast<E>(ei);
        const auto gs = guards_of(e);
        o << "    {conn_event::" << ename(e) << ", \"" << ename(e) << "\", " << gs.size() << ", {";
        for (size_t i = 0; i < gs.size(); ++i) o << (i ? ", " : "") << "guard::" << gname(gs[i]);
        o << "}},\n";
    }
    o << "};\n\n"
         "struct cell_row {\n    conn_state_id from;\n    conn_event    event;\n    guard         g;\n    conn_state_id to;\n    close_action  action;\n    uint8_t       fx;\n};\n"
         "static const cell_row kCellRows[] = {\n";
    for (const auto& c : t.d.cells)
        o << "    {conn_state_id::" << state_name(t.d.states[c.from]) << ", conn_event::" << ename(c.e) << ", guard::" << gname(c.g)
          << ", conn_state_id::" << state_name(t.d.states[c.to]) << ", close_action::" << action_name(c.action) << ", " << fx_expr(c.fx) << "},\n";
    o << "};\n";
    return o.str();
}

// ------------------------------------------------------------------ docs
static std::string delta(const Axes& a, const Axes& b) {
    std::string s;
    auto add = [&](const std::string& x) { if (!s.empty()) s += "; "; s += x; };
    if (a.phase != b.phase) add(std::string("phase ") + phase_name(a.phase) + "→" + phase_name(b.phase));
    if (a.read != b.read) add(std::string("read ") + read_name(a.read) + "→" + read_name(b.read));
    if (a.write != b.write) add(std::string("write ") + (a.write ? "on→off" : "off→on"));
    if (a.in_round != b.in_round) add(std::string("round ") + (a.in_round ? "on→off" : "off→on"));
    if (a.round != b.round) add(std::string("owner ") + round_name(a.round) + "→" + round_name(b.round));
    return s.empty() ? "—" : s;
}

static std::string emit_doc(const Table& t) {
    std::ostringstream o;
    o << "# Connection state machine\n\n"
         "*Generated by `tools/gen_conn_fsm.cpp` from the rules in `tools/conn_fsm_spec.h`, together with the\n"
         "transition function in `tcp-server-mt.h` and `verify/conn_fsm_cases.inc`. Do not edit.*\n\n"
         "## States\n\n"
         "A connection's state is one value of `conn_state_id`, a closed enum of the **" << t.d.states.size()
      << "** legal combinations of five axes. A combination the rules forbid has no enumerator, so it cannot be\n"
         "constructed. The rules, applied to the 180 combinations:\n\n"
         "1. `round = close` needs `phase ≠ open`.\n"
         "2. `round = peer_closed` needs `read = eof`.\n"
         "3. `read = paused` needs `phase ∈ {open, draining}` and no serving round.\n"
         "4. `closed` has no write interest, no serving round and no pause.\n"
         "5. A serving round (`in_round`) needs `phase ≠ open`.\n\n"
         "| phase | states |\n|---|---|\n";
    int per[5] = {0, 0, 0, 0, 0};
    for (auto& a : t.d.states) ++per[static_cast<int>(a.phase)];
    for (int i = 0; i < 5; ++i) o << "| `" << phase_name(static_cast<conn_phase>(i)) << "` | " << per[i] << " |\n";
    o << "\nNames are `<phase>_<read>[_<flags>]` with `read` ∈ `act`, `pau`, `eof` and flags in the order\n"
         "`R` serving round on the stack, `C` round owned by the close, `P` round owned by the peer's FIN,\n"
         "`W` write interest armed.\n\n"
         "Phases only move forward (`open → draining → closing → fin_sent → closed`; `open`/`draining` may go\n"
         "straight to `closing`; any phase may go to `closed`), and `closed` is terminal.\n\n"
         "### Payloads\n\n"
         "Data that belongs to a state, not a state axis. It exists only where the state carries it:\n\n"
         "| payload | carried by | meaning |\n|---|---|---|\n"
         "| stamp (paused) | every `read = paused` state | when EPOLLIN was dropped |\n"
         "| stamp (close) | every `closing` and `fin_sent` state | when the close was parked |\n"
         "| budget | `open`: never; `draining`: always; `closing`, `fin_sent`, `closed`: optional | input a graceful close still owes the peer, measured once |\n\n"
         "The two stamps share one slot, because `paused` and `closing`/`fin_sent` never coexist (rule 3).\n\n"
         "## Events and guards\n\n"
         "| event | issued when | guards |\n|---|---|---|\n"
         "| `pause` | read-side backpressure | — |\n"
         "| `resume` | pending output fell below the low mark | — |\n"
         "| `eof` | `recv()` returned 0 | — |\n"
         "| `write_on`, `write_off` | EPOLLOUT wanted / no longer wanted | — |\n"
         "| `graceful` | a close that parks now (idle timeout, close with nothing to serve) | — |\n"
         "| `request` | a public close (`close_connection`) | — |\n"
         "| `round_begin`, `round_end` | around a serving round | — |\n"
         "| `input_drained` | a serving round finished | `more`, `served`, `idle_owed`, `idle_clear` |\n"
         "| `input_defer_failed` | the hand-off for another round failed | — |\n"
         "| `peer_fin` | the peer's FIN, after `eof` | `wbuf_busy`, `owed`, `clear` |\n"
         "| `fin_sent` | `shutdown(SHUT_WR)` succeeded | — |\n"
         "| `closed` | teardown | — |\n\n"
         "Preconditions: `round_begin` needs `read ≠ paused`; `peer_fin` needs `read = eof`; `fin_sent` needs\n"
         "`closing`; `input_drained` and `input_defer_failed` need a requested close (so `phase ≠ open`). Outside\n"
         "its precondition an event leaves the state alone.\n\n"
         "Guard classes (the reduction of the facts the decision reads; checked lossless against the oracle):\n\n"
         "| guard | meaning |\n|---|---|\n"
         "| `more` | served > 0, rbuf still holds input, budget left |\n"
         "| `served` | served > 0 otherwise |\n"
         "| `idle_owed` | served = 0, replies still owed (`pending > 0`) |\n"
         "| `idle_clear` | served = 0, nothing owed |\n"
         "| `wbuf_busy` | wbuf holds bytes: not our turn yet |\n"
         "| `owed` | wbuf empty, replies still reserved |\n"
         "| `clear` | wbuf empty, nothing owed |\n\n"
         "Effects: `resume` reads were resumed (the reactor applies it); `stamp` the state's stamp := now;\n"
         "`measure` an unmeasured budget := bytes buffered; `charge` budget := max(0, budget − dealt).\n\n"
         "## State table\n\n"
         "| id | phase | read | write | round on stack | owner |\n|---|---|---|---|---|---|\n";
    for (const auto& a : t.d.states)
        o << "| `" << state_name(a) << "` | " << phase_name(a.phase) << " | " << read_name(a.read) << " | " << (a.write ? "armed" : "—")
          << " | " << (a.in_round ? "yes" : "—") << " | " << (a.round == close_round::none ? "—" : round_name(a.round)) << " |\n";
    o << "\n## Transitions\n\n"
         "Only cells that change something are listed; every other in-domain cell leaves the state alone, does\n"
         "nothing, and has no effects.\n\n";
    for (int ei = 0; ei < kEvents; ++ei) {
        const E e = static_cast<E>(ei);
        long total = 0, changing = 0;
        for (const auto& c : t.d.cells) if (c.e == e) { ++total; if (c.to != c.from || c.action != close_action::none || c.fx) ++changing; }
        o << "### `" << ename(e) << "`\n\n" << total << " cells, " << changing << " change something.\n\n";
        if (!changing) { o << "\n"; continue; }
        const bool guarded = guards_of(e).size() > 1;
        o << (guarded ? "| from | guard | to | changes | action | effects |\n|---|---|---|---|---|---|\n"
                      : "| from | to | changes | action | effects |\n|---|---|---|---|---|\n");
        for (const auto& c : t.d.cells) {
            if (c.e != e) continue;
            if (c.to == c.from && c.action == close_action::none && c.fx == 0) continue;
            o << "| `" << state_name(t.d.states[c.from]) << "` | ";
            if (guarded) o << "`" << gname(c.g) << "` | ";
            o << "`" << state_name(t.d.states[c.to]) << "` | " << delta(t.d.states[c.from], t.d.states[c.to]) << " | "
              << (c.action == close_action::none ? "—" : std::string("`") + action_name(c.action) + "`") << " | " << (c.fx ? fx_words(c.fx) : "—") << " |\n";
        }
        o << "\n";
    }
    return o.str();
}

// ------------------------------------------------------------------ io
static std::string slurp(const std::string& p) { std::ifstream f(p, std::ios::binary); std::stringstream s; s << f.rdbuf(); return s.str(); }
static bool spit(const std::string& p, const std::string& text) { std::ofstream f(p, std::ios::binary); f << text; return bool(f); }

// Line endings. The generator works in LF and writes each file back in the line
// endings it already has, so a CRLF checkout gets a CRLF header (not a mixed one)
// and --check compares text, not bytes of terminators.
static bool uses_crlf(const std::string& t) { return t.find("\r\n") != std::string::npos; }
static std::string to_lf(const std::string& t) {
    std::string o; o.reserve(t.size());
    for (char c : t) if (c != '\r') o += c;
    return o;
}
static std::string with_eol(const std::string& lf, bool crlf) {
    if (!crlf) return lf;
    std::string o; o.reserve(lf.size() + lf.size() / 30);
    for (char c : lf) { if (c == '\n') o += '\r'; o += c; }
    return o;
}

static bool splice(const std::string& header, const std::string& region, std::string* out) {
    const size_t b = header.find(kBegin);
    const size_t e = header.find(kEnd);
    if (b == std::string::npos || e == std::string::npos || e < b) return false;
    const size_t after = header.find('\n', e);
    *out = header.substr(0, b) + region + (after == std::string::npos ? "" : header.substr(after + 1));
    return true;
}

int main(int argc, char** argv) {
    std::string header, cases, doc, fragment;
    bool check = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", a.c_str()); std::exit(2); } return argv[++i]; };
        if (a == "--header") header = next(); else if (a == "--cases") cases = next();
        else if (a == "--doc") doc = next(); else if (a == "--fragment") fragment = next();
        else if (a == "--check") check = true;
        else { std::fprintf(stderr, "unknown argument %s\n", a.c_str()); return 2; }
    }
    Table t;
    t.d = derive_all<SpecOracle>();
    for (const auto& c : t.d.cells) t.by_key[std::make_tuple(c.from, static_cast<int>(c.e), static_cast<int>(c.g))] = &c;
    check_structure(t);
    check_guards(t);
    if (errors_seen) { std::fprintf(stderr, "generator: %d problem(s); nothing written\n", errors_seen); return 1; }
    std::printf("generator: %zu cells derived (%d events)\n", t.d.cells.size(), kEvents);

    const std::string region = emit_region(t);
    int stale = 0;
    auto put = [&](const std::string& path, const std::string& text) {
        if (path.empty()) return;
        const std::string existing = slurp(path);
        const bool crlf = uses_crlf(existing);
        if (check) { if (to_lf(existing) != text) { std::fprintf(stderr, "generator: %s is out of date\n", path.c_str()); ++stale; } }
        else if (!spit(path, with_eol(text, crlf))) { std::fprintf(stderr, "generator: cannot write %s\n", path.c_str()); ++stale; }
        else std::printf("generator: wrote %s\n", path.c_str());
    };
    if (!header.empty()) {
        std::string out;
        if (!splice(to_lf(slurp(header)), region, &out)) { std::fprintf(stderr, "generator: %s has no BEGIN/END GENERATED markers\n", header.c_str()); return 1; }
        put(header, out);
    }
    put(fragment, region);
    put(cases, emit_cases(t));
    put(doc, emit_doc(t));
    return stale ? 1 : 0;
}
