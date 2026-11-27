// tools/conn_fsm_model.h -- what the generator knows about the machine, and how
// it learns the rest by running the oracle.
//
// Legality is the five discrete negative rules and nothing else. Everything a
// transition does is *observed* by running an oracle (the five-axis machine in
// conn_fsm_spec.h) on a state built from each legal combination, so the table is
// derived, not transcribed. The derivation is a template over the oracle so the
// same code can be pointed at the spec and, before it is deleted, at the
// header's own conn_sm.
#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "conn_fsm_spec.h"

namespace model {

using industrial::close_action;
using industrial::close_round;
using industrial::conn_phase;
using industrial::read_mode;
using industrial::read_resume;

// ------------------------------------------------------------------ axes
struct Axes {
    conn_phase  phase = conn_phase::open;
    read_mode   read  = read_mode::active;
    bool        write = false;
    bool        in_round = false;
    close_round round = close_round::none;
    bool operator==(const Axes& o) const {
        return phase == o.phase && read == o.read && write == o.write &&
               in_round == o.in_round && round == o.round;
    }
};

// The five discrete negative rules. A combination is a state iff none applies.
inline bool legal(const Axes& a) {
    if (a.round == close_round::close && a.phase == conn_phase::open) return false;
    if (a.round == close_round::peer_closed && a.read != read_mode::eof) return false;
    if (a.read == read_mode::paused &&
        ((a.phase != conn_phase::open && a.phase != conn_phase::draining) || a.in_round))
        return false;
    if (a.phase == conn_phase::closed &&
        (a.write || a.in_round || a.read == read_mode::paused))
        return false;
    if (a.in_round && a.phase == conn_phase::open) return false;
    return true;
}

inline const char* phase_name(conn_phase p) {
    static const char* n[] = {"open", "draining", "closing", "fin_sent", "closed"};
    return n[static_cast<int>(p)];
}
inline const char* read_name(read_mode r) {
    static const char* n[] = {"active", "paused", "eof"};
    return n[static_cast<int>(r)];
}
inline const char* round_name(close_round r) {
    static const char* n[] = {"none", "close", "peer_closed"};
    return n[static_cast<int>(r)];
}
inline const char* action_name(close_action a) {
    using A = close_action;
    switch (a) {
        case A::none: return "none";
        case A::drain_input: return "drain_input";
        case A::drain_wbuf: return "drain_wbuf";
        case A::drain_complete: return "drain_complete";
        case A::send_fin: return "send_fin";
        case A::defer_drain_input: return "defer_drain_input";
        case A::defer_park: return "defer_park";
        case A::defer_peer_closed: return "defer_peer_closed";
        case A::close_graceful: return "close_graceful";
        case A::close_abort: return "close_abort";
    }
    return "?";
}

// <phase>_<read:act|pau|eof>[_<flags>], flags in a fixed order:
//   R in_round, C round=close, P round=peer_closed, W write_armed.
inline std::string state_name(const Axes& a) {
    static const char* rd[] = {"act", "pau", "eof"};
    std::string n = std::string(phase_name(a.phase)) + "_" + rd[static_cast<int>(a.read)];
    std::string f;
    if (a.in_round) f += 'R';
    if (a.round == close_round::close) f += 'C';
    if (a.round == close_round::peer_closed) f += 'P';
    if (a.write) f += 'W';
    if (!f.empty()) n += "_" + f;
    return n;
}

// Enumeration order is the id order: phase, read, in_round, round, write.
inline std::vector<Axes> all_states() {
    std::vector<Axes> v;
    for (int p = 0; p < 5; ++p)
        for (int r = 0; r < 3; ++r)
            for (int ir = 0; ir < 2; ++ir)
                for (int rn = 0; rn < 3; ++rn)
                    for (int w = 0; w < 2; ++w) {
                        Axes a;
                        a.phase = static_cast<conn_phase>(p);
                        a.read = static_cast<read_mode>(r);
                        a.in_round = ir;
                        a.round = static_cast<close_round>(rn);
                        a.write = w;
                        if (legal(a)) v.push_back(a);
                    }
    return v;
}

// ------------------------------------------------------------------ events/guards
enum class E : int {
    pause, resume, eof, write_on, write_off, graceful, request, round_begin,
    round_end, input_drained, input_defer_failed, peer_fin, fin_sent, closed,
    COUNT
};
enum class G : int {
    none, more, served, idle_owed, idle_clear, wbuf_busy, owed, clear, COUNT
};
constexpr int kEvents = static_cast<int>(E::COUNT);

inline const char* ename(E e) {
    static const char* n[] = {"pause", "resume", "eof", "write_on", "write_off", "graceful",
                              "request", "round_begin", "round_end", "input_drained",
                              "input_defer_failed", "peer_fin", "fin_sent", "closed"};
    return n[static_cast<int>(e)];
}
inline const char* gname(G g) {
    static const char* n[] = {"none", "more", "served", "idle_owed", "idle_clear",
                              "wbuf_busy", "owed", "clear"};
    return n[static_cast<int>(g)];
}
inline std::vector<G> guards_of(E e) {
    if (e == E::input_drained) return {G::more, G::served, G::idle_owed, G::idle_clear};
    if (e == E::peer_fin) return {G::wbuf_busy, G::owed, G::clear};
    return {G::none};
}
// The states an event may be issued from (its precondition, as a function of
// the state alone). Outside it the generated function leaves the state alone.
inline bool in_domain(E e, const Axes& a) {
    switch (e) {
        case E::round_begin: return a.read != read_mode::paused;
        case E::peer_fin: return a.read == read_mode::eof;
        case E::fin_sent: return a.phase == conn_phase::closing;
        case E::input_drained:
        case E::input_defer_failed: return a.phase != conn_phase::open;
        default: return true;
    }
}

// The reduction of conn_facts (and the served/budget parameters) to a guard.
// The generator emits these two functions verbatim into the header; this is
// the copy it checks the oracle against.
inline G input_guard(bool served, bool rbuf_nonempty, bool budget_left, bool pending) {
    if (served) return (rbuf_nonempty && budget_left) ? G::more : G::served;
    return pending ? G::idle_owed : G::idle_clear;
}
inline G peer_guard(bool wbuf_empty, bool pending) {
    if (!wbuf_empty) return G::wbuf_busy;
    return pending ? G::owed : G::clear;
}

// ------------------------------------------------------------------ effects
namespace fx {
constexpr uint8_t stamp = 1;    // payload stamp := now
constexpr uint8_t measure = 2;  // unmeasured budget := rbuf_readable
constexpr uint8_t charge = 4;   // budget := max(0, budget - dealt)
constexpr uint8_t resume = 8;   // the reactor must apply a read resume
}  // namespace fx
inline std::string fx_expr(uint8_t f) {
    if (f == 0) return "0";
    std::string s;
    auto add = [&](uint8_t bit, const char* n) {
        if (f & bit) { if (!s.empty()) s += " | "; s += n; }
    };
    add(fx::stamp, "conn_fx::stamp");
    add(fx::measure, "conn_fx::measure");
    add(fx::charge, "conn_fx::charge");
    add(fx::resume, "conn_fx::resume");
    return s;
}
inline std::string fx_words(uint8_t f) {
    if (f == 0) return "";
    std::string s;
    auto add = [&](uint8_t bit, const char* n) {
        if (f & bit) { if (!s.empty()) s += ", "; s += n; }
    };
    add(fx::resume, "resume");
    add(fx::stamp, "stamp");
    add(fx::measure, "measure");
    add(fx::charge, "charge");
    return s;
}

// ------------------------------------------------------------------ derivation
constexpr int64_t kPausedStamp = 7000;   // paused_at_ms sentinel
constexpr int64_t kCloseStamp = 8000;    // close_started_at_ms sentinel
constexpr int64_t kNow = 4242;
constexpr int64_t kRbufReadable = 999;

struct In {
    int64_t budget = -1;
    int64_t now = kNow;
    bool    wbuf_empty = true;
    bool    rbuf_empty = true;
    int64_t rbuf_readable = 0;
    size_t  pending = 0;
    int     served = 0;
    int64_t dealt = 0;
};

struct Out {
    std::string err;                 // empty when the run was well-formed
    Axes        next;
    close_action action = close_action::none;
    bool        resumed = false;
    int64_t     paused_for = 0;
    int64_t     budget_after = -1;
    int64_t     stamp_after = 0;     // payload stamp of the next state, if it carries one
    bool        next_carries = false;
    bool        old_valid_after = true;   // the oracle's own full invariant
};

inline bool carries_stamp(const Axes& a) {
    return a.read == read_mode::paused || a.phase == conn_phase::closing ||
           a.phase == conn_phase::fin_sent;
}

template <class S>
S make_state(const Axes& a, int64_t budget) {
    S s;
    s.phase = a.phase;
    s.read = a.read;
    s.write_armed = a.write;
    s.in_round = a.in_round;
    s.round = a.round;
    s.paused_at_ms = a.read == read_mode::paused ? kPausedStamp : 0;
    s.close_started_at_ms =
        (a.phase == conn_phase::closing || a.phase == conn_phase::fin_sent) ? kCloseStamp : 0;
    s.close_input_bytes = budget;
    return s;
}
template <class S>
Axes axes_of(const S& s) {
    Axes a;
    a.phase = s.phase; a.read = s.read; a.write = s.write_armed;
    a.in_round = s.in_round; a.round = s.round;
    return a;
}

// Forwarders so one derivation can drive either oracle.
#define CONN_FSM_ORACLE(NAME, STATE, NS)                                                       \
    struct NAME {                                                                              \
        using state = STATE;                                                                   \
        static bool on_pause(state& s, int64_t n) { return NS::on_pause(s, n); }               \
        static industrial::read_resume on_resume(state& s, int64_t n) { return NS::on_resume(s, n); }      \
        static void on_eof(state& s) { NS::on_eof(s); }                                        \
        static bool on_set_write(state& s, bool a) { return NS::on_set_write(s, a); }          \
        static industrial::close_step on_graceful(state& s, int64_t n) { return NS::on_graceful(s, n); } \
        static industrial::close_step on_request(state& s, const industrial::conn_facts& f, int64_t n) { return NS::on_request(s, f, n); } \
        static void on_round_begin(state& s) { NS::on_round_begin(s); }                        \
        static void on_round_end(state& s) { NS::on_round_end(s); }                            \
        static industrial::close_action on_input_drained(state& s, const industrial::conn_facts& f, int served, int64_t dealt) { return NS::on_input_drained(s, f, served, dealt); } \
        static industrial::close_action on_input_defer_failed(state& s) { return NS::on_input_defer_failed(s); } \
        static industrial::close_action on_peer_fin(state& s, const industrial::conn_facts& f) { return NS::on_peer_fin(s, f); } \
        static void on_fin_sent(state& s) { NS::on_fin_sent(s); }                              \
        static bool on_closed(state& s) { return NS::on_closed(s); }                           \
        static bool valid(const state& s) { return NS::valid(s); }                             \
    }

CONN_FSM_ORACLE(SpecOracle, conn_fsm_spec::state, conn_fsm_spec::sm);

// Run one event on one state with the given inputs and report what happened.
template <class Ops>
Out run(const Axes& a, E e, const In& in) {
    using S = typename Ops::state;
    S s = make_state<S>(a, in.budget);
    industrial::conn_facts f;
    f.wbuf_empty = in.wbuf_empty;
    f.rbuf_empty = in.rbuf_empty;
    f.rbuf_readable = static_cast<size_t>(in.rbuf_readable);
    f.pending = in.pending;
    close_action act = close_action::none;
    read_resume rr;
    switch (e) {
        case E::pause: Ops::on_pause(s, in.now); break;
        case E::resume: rr = Ops::on_resume(s, in.now); break;
        case E::eof: Ops::on_eof(s); break;
        case E::write_on: Ops::on_set_write(s, true); break;
        case E::write_off: Ops::on_set_write(s, false); break;
        case E::graceful: { auto st = Ops::on_graceful(s, in.now); act = st.action; rr = st.resume; break; }
        case E::request: { auto st = Ops::on_request(s, f, in.now); act = st.action; rr = st.resume; break; }
        case E::round_begin: Ops::on_round_begin(s); break;
        case E::round_end: Ops::on_round_end(s); break;
        case E::input_drained: act = Ops::on_input_drained(s, f, in.served, in.dealt); break;
        case E::input_defer_failed: act = Ops::on_input_defer_failed(s); break;
        case E::peer_fin: act = Ops::on_peer_fin(s, f); break;
        case E::fin_sent: Ops::on_fin_sent(s); break;
        case E::closed: Ops::on_closed(s); break;
        case E::COUNT: break;
    }
    Out o;
    o.next = axes_of(s);
    o.action = act;
    o.resumed = rr.resumed;
    o.paused_for = rr.paused_for_ms;
    o.budget_after = s.close_input_bytes;
    o.next_carries = carries_stamp(o.next);
    if (o.next.read == read_mode::paused) o.stamp_after = s.paused_at_ms;
    else if (o.next.phase == conn_phase::closing || o.next.phase == conn_phase::fin_sent)
        o.stamp_after = s.close_started_at_ms;
    o.old_valid_after = Ops::valid(s);
    return o;
}

// Which payload effects did that run perform? Also checks that the payloads
// moved the way the machine's structure says they may.
inline uint8_t classify(const Axes& a, E e, const In& in, const Out& o, std::string* err) {
    uint8_t f = 0;
    auto fail = [&](const std::string& m) { if (err && err->empty()) *err = m; };
    // resume
    if (o.resumed) {
        f |= fx::resume;
        if (a.read != read_mode::paused) fail("resume reported for a connection that was not paused");
        else if (o.paused_for != in.now - kPausedStamp) fail("paused interval is not now - paused_at");
    } else if (a.read == read_mode::paused && o.next.read == read_mode::active &&
               (e == E::resume || e == E::graceful || e == E::request)) {
        fail("a paused connection left paused without reporting a resume");
    }
    // stamp
    const bool src_closing = a.phase == conn_phase::closing || a.phase == conn_phase::fin_sent;
    const bool next_paused = o.next.read == read_mode::paused;
    const bool next_closing = o.next.phase == conn_phase::closing || o.next.phase == conn_phase::fin_sent;
    if (o.next_carries) {
        if (o.stamp_after == in.now) f |= fx::stamp;
        else {
            const bool kept = (next_paused && a.read == read_mode::paused && o.stamp_after == kPausedStamp) ||
                              (next_closing && src_closing && o.stamp_after == kCloseStamp);
            if (!kept) fail("a stamp-carrying state has a stamp that is neither now nor carried over");
        }
    }
    // budget
    if (o.budget_after != in.budget) {
        if (in.budget < 0 && e == E::request && o.budget_after == in.rbuf_readable) f |= fx::measure;
        else if (in.budget >= 0 && e == E::input_drained && in.dealt > 0 &&
                 o.budget_after == std::max<int64_t>(0, in.budget - in.dealt)) f |= fx::charge;
        else fail("budget changed in a way that is neither measure nor charge");
    }
    if (!o.old_valid_after) fail("the oracle's own invariant broke");
    return f;
}

// ------------------------------------------------------------------ cells
struct Cell {
    int from = 0;
    E e = E::pause;
    G g = G::none;
    int to = 0;
    close_action action = close_action::none;
    uint8_t fx = 0;
};

inline std::vector<int64_t> budget_variants(const Axes& a, E e) {
    if (a.phase == conn_phase::open) return {-1};            // open never has a budget
    if (a.phase == conn_phase::draining) return {500};        // draining always has one
    if (e == E::input_drained || e == E::input_defer_failed) return {500};   // precondition: measured
    return {-1, 500};                                         // later phases: optional
}

// Representative inputs for a guard class.
inline In canonical(E e, G g, int64_t budget) {
    In in; in.budget = budget;
    if (e == E::request) { in.rbuf_empty = false; in.rbuf_readable = kRbufReadable; }
    if (e == E::input_drained) {
        in.dealt = 6;
        switch (g) {
            case G::more:       in.served = 3; in.rbuf_empty = false; in.rbuf_readable = 10; break;
            case G::served:     in.served = 3; in.rbuf_empty = true; break;
            case G::idle_owed:  in.served = 0; in.dealt = 0; in.pending = 1; break;
            case G::idle_clear: in.served = 0; in.dealt = 0; in.pending = 0; break;
            default: break;
        }
    }
    if (e == E::peer_fin) {
        switch (g) {
            case G::wbuf_busy: in.wbuf_empty = false; break;
            case G::owed:      in.wbuf_empty = true; in.pending = 1; break;
            case G::clear:     in.wbuf_empty = true; in.pending = 0; break;
            default: break;
        }
    }
    return in;
}

struct Derived {
    std::vector<Axes> states;
    std::vector<Cell> cells;          // in-domain cells only, in (event, guard, state) order
    std::vector<std::string> errors;
};

template <class Ops>
Derived derive_all() {
    Derived d;
    d.states = all_states();
    std::map<std::string, int> index;
    for (size_t i = 0; i < d.states.size(); ++i) index[state_name(d.states[i])] = static_cast<int>(i);
    auto idx = [&](const Axes& a) -> int {
        auto it = index.find(state_name(a));
        return it == index.end() || !legal(a) ? -1 : it->second;
    };
    for (int ei = 0; ei < kEvents; ++ei) {
        const E e = static_cast<E>(ei);
        for (G g : guards_of(e)) {
            for (size_t si = 0; si < d.states.size(); ++si) {
                const Axes& a = d.states[si];
                if (!in_domain(e, a)) continue;
                Cell c; c.from = static_cast<int>(si); c.e = e; c.g = g;
                bool first = true;
                for (int64_t b : budget_variants(a, e)) {
                    const In in = canonical(e, g, b);
                    const Out o = run<Ops>(a, e, in);
                    std::string err;
                    const uint8_t f = classify(a, e, in, o, &err);
                    const int to = idx(o.next);
                    const std::string where = std::string(ename(e)) + "/" + gname(g) + " from " + state_name(a);
                    if (!err.empty()) d.errors.push_back(where + ": " + err);
                    if (to < 0) { d.errors.push_back(where + ": lands in a combination the five rules forbid"); continue; }
                    if (first) { c.to = to; c.action = o.action; c.fx = f; first = false; }
                    else {
                        if (c.to != to || c.action != o.action)
                            d.errors.push_back(where + ": the budget variants disagree on the next state or action");
                        c.fx |= f;
                    }
                }
                if (!first) d.cells.push_back(c);
            }
        }
    }
    return d;
}

}  // namespace model
