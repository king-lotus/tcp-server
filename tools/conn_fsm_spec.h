// tools/conn_fsm_spec.h -- the specification of the connection state machine.
//
// This is the five-axis machine exactly as it stood in tcp-server-mt.h before
// the states were made a closed enum (conn_state_id): the same state struct, the
// same transition functions, relocated verbatim and renamed (conn_state ->
// state, conn_sm -> sm). It is not shipped: it is the oracle the generator
// derives its table from, and the oracle the test diffs every generated cell
// against. If a rule of the machine changes, it changes HERE and the generator
// regenerates the header, the test cases and docs/conn-fsm.md together.
//
// The five discrete axes are phase, read, write_armed, in_round and round. The
// payload fields (close_input_bytes, close_started_at_ms, paused_at_ms) are
// data attached to states, not axes; the generated machine stores them as
// payloads of the states that carry them.
#pragma once
// The spec needs only the header's vocabulary (enums, conn_facts, close_step), so
// by default it asks for just that: the generator must build before the
// generated region of the header exists. A different header can be named with
// -DCONN_FSM_HEADER.
#ifndef CONN_FSM_HEADER
#define INDUSTRIAL_CONN_FSM_VOCABULARY_ONLY
#define CONN_FSM_HEADER "../include/tcp-server-mt.h"
#endif
#include CONN_FSM_HEADER

namespace conn_fsm_spec {

using industrial::close_action;
using industrial::close_round;
using industrial::close_step;
using industrial::conn_facts;
using industrial::conn_phase;
using industrial::drain_outcome;
using industrial::read_mode;
using industrial::read_resume;

struct state {
    conn_phase  phase = conn_phase::open;
    read_mode   read  = read_mode::active;
    bool        write_armed = false;
    // A drain_input_then_close() serving round is on the stack. Read-side
    // backpressure does not apply inside one: pausing there is what made a
    // close requested while the connection was read-paused serve nothing, park,
    // and FIN over the requests already sitting in rbuf.
    bool        in_round = false;
    close_round round = close_round::none;
    // Input this graceful close still owes the peer: the bytes buffered and
    // unserved when the close was first requested. Negative means "not
    // measured yet".
    int64_t     close_input_bytes = -1;
    int64_t     close_started_at_ms = 0;  // when the close was parked
    int64_t     paused_at_ms = 0;         // when EPOLLIN was dropped, else 0
};

namespace sm {

inline bool is_closing(const state& s) noexcept {
    return s.phase == conn_phase::closing || s.phase == conn_phase::fin_sent;
}
// Nothing left to park: FIN is out, or the connection is gone.
inline bool is_done(const state& s) noexcept {
    return s.phase == conn_phase::fin_sent || s.phase == conn_phase::closed;
}
// Whether read-side backpressure may act: not once a close is parked, and not
// inside a serving round.
inline bool backpressure_applies(const state& s) noexcept {
    return (s.phase == conn_phase::open || s.phase == conn_phase::draining) &&
           !s.in_round;
}

// The invariant every transition preserves, as the name of the first clause a
// state breaks (nullptr when it breaks none). Anything this names is a
// combination of fields that no sequence of events can produce.
inline const char* violation(const state& s) noexcept {
    const conn_phase ph = s.phase;
    if ((s.read == read_mode::paused) != (s.paused_at_ms != 0))
        return "paused <=> pause timestamp";
    if (s.read == read_mode::paused &&
        ((ph != conn_phase::open && ph != conn_phase::draining) || s.in_round))
        return "paused only in open/draining, outside a serving round";
    if (ph == conn_phase::closed &&
        (s.write_armed || s.in_round || s.read == read_mode::paused))
        return "closed is terminal and normalised";
    if (s.in_round && ph == conn_phase::open)
        return "a serving round needs a requested close";
    if (ph == conn_phase::open && s.close_input_bytes >= 0)
        return "open has no measured input budget";
    if (ph == conn_phase::draining && s.close_input_bytes < 0)
        return "draining has a measured input budget";
    if (is_closing(s) && s.close_started_at_ms == 0)
        return "the close clock runs while parked";
    if ((ph == conn_phase::open || ph == conn_phase::draining) &&
        s.close_started_at_ms != 0)
        return "the close clock is off before parking";
    if (s.round == close_round::close && ph == conn_phase::open)
        return "a close-owned round needs a requested close";
    if (s.round == close_round::peer_closed && s.read != read_mode::eof)
        return "a peer-owned round needs the peer's FIN";
    return nullptr;
}
inline bool valid(const state& s) noexcept {
    return violation(s) == nullptr;
}

// ----- read side -----

// Take EPOLLIN away. False when it may not (already paused, peer FIN seen,
// closed, parked, or inside a serving round): the caller does nothing.
inline bool on_pause(state& s, int64_t now) noexcept {
    if (s.read != read_mode::active || !backpressure_applies(s)) return false;
    s.read = read_mode::paused;
    s.paused_at_ms = now != 0 ? now : 1;
    return true;
}

inline read_resume on_resume(state& s, int64_t now) noexcept {
    read_resume r;
    if (s.read != read_mode::paused) return r;
    s.read = read_mode::active;
    r.resumed = true;
    r.paused_for_ms = s.paused_at_ms != 0 ? now - s.paused_at_ms : 0;
    s.paused_at_ms = 0;
    return r;
}

// recv() returned 0.
inline void on_eof(state& s) noexcept {
    s.read = read_mode::eof;
    s.paused_at_ms = 0;
}

// ----- write side -----

// Target-state, not a toggle. False when nothing changed (or the connection is
// gone, where write interest means nothing).
inline bool on_set_write(state& s, bool armed) noexcept {
    if (s.phase == conn_phase::closed || s.write_armed == armed) return false;
    s.write_armed = armed;
    return true;
}

// The epoll interest set this state asks for. The only place it is derived.
inline uint32_t interest(const state& s) noexcept {
    uint32_t ev = 0;
    if (s.read == read_mode::active) ev |= EPOLLIN;
    if (s.write_armed) ev |= EPOLLOUT;
    return ev;
}

// ----- close protocol -----

// A graceful close that parks now (close_conn graceful, idle timeout).
// Idempotent: once closed or FINed there is nothing left to park. Entering
// closing resumes a paused connection -- the combination is not a state.
inline close_step on_graceful(state& s, int64_t now) noexcept {
    close_step st;
    if (is_done(s)) return st;
    if (s.phase != conn_phase::closing) {
        s.phase = conn_phase::closing;
        s.close_started_at_ms = now != 0 ? now : 1;
        st.resume = on_resume(s, now);
    }
    st.action = close_action::drain_wbuf;
    return st;
}

// A public close (close_connection / stop_gracefully): serve buffered input
// first, so answers to already-arrived requests still reach the peer. Resumes
// a paused connection, because serving the backlog is the whole point.
inline close_step on_request(state& s, const conn_facts& f,
                             int64_t now) noexcept {
    close_step st;
    if (is_done(s)) return st;
    if (s.close_input_bytes < 0)
        s.close_input_bytes = static_cast<int64_t>(f.rbuf_readable);
    if (s.phase == conn_phase::open) s.phase = conn_phase::draining;
    st.resume = on_resume(s, now);
    st.action = close_action::drain_input;
    return st;
}

// Bracket a serving round. A no-op outside draining/closing, where a round
// cannot exist.
inline void on_round_begin(state& s) noexcept {
    // requires: reads are not paused -- on_request resumed them.
    assert(s.read != read_mode::paused);
    if (s.phase == conn_phase::draining || s.phase == conn_phase::closing)
        s.in_round = true;
}
inline void on_round_end(state& s) noexcept { s.in_round = false; }

// The park decision shared by on_input_drained and on_input_defer_failed:
// peer_closed owns the round; an already-close round (or a round that served
// nothing) parks now; otherwise defer one park round.
inline close_action park_decision(state& s, int served) noexcept {
    if (s.round == close_round::peer_closed) return close_action::none;
    if (served == 0 || s.round == close_round::close)
        return close_action::drain_wbuf;
    s.round = close_round::close;
    return close_action::defer_park;
}

// A serving round finished. served is the frame count, dealt the bytes this
// round took off the close_input_bytes budget.
inline close_action on_input_drained(state& s, const conn_facts& f,
                                     int served, int64_t dealt) noexcept {
    if (is_done(s)) return close_action::none;
    // requires: a close was requested (on_request measured the budget).
    assert(s.close_input_bytes >= 0);
    // Clamped at zero, not allowed to go negative: negative is the "not
    // measured yet" sentinel, and an exhausted budget that read as unmeasured
    // let a later close request re-measure it and hand a peer that keeps
    // sending a fresh budget -- the very thing the budget exists to prevent.
    if (served > 0 && dealt > 0)
        s.close_input_bytes = std::max<int64_t>(0, s.close_input_bytes - dealt);
    if (served > 0 && !f.rbuf_empty && s.close_input_bytes > 0)
        return close_action::defer_drain_input;
    if (served == 0 && s.round == close_round::none && f.pending > 0) {
        s.round = close_round::close;
        return close_action::defer_park;
    }
    return park_decision(s, served);
}

// Re-decide after the input-defer hand-off failed: drain_input is off the table
// and served was > 0, so what remains is the park decision.
inline close_action on_input_defer_failed(state& s) noexcept {
    // requires: on_input_drained just answered defer_drain_input.
    assert(s.close_input_bytes >= 0);
    return park_decision(s, 1);
}

// What to do after a wbuf drain. dead/error end the connection; stalled leaves
// the rest to a later EPOLLOUT; drained means wbuf is empty and the close
// decision (FIN or otherwise) is made by the caller.
inline close_action on_drain_result(drain_outcome r) noexcept {
    if (r == drain_outcome::error || r == drain_outcome::dead)
        return close_action::close_abort;
    if (r == drain_outcome::drained) return close_action::drain_complete;
    return close_action::none;  // stalled
}

// The peer FINed (read is already eof). A productive wbuf means it is not our
// turn yet: the caller parks, handle_write resumes this once the bytes are out.
// defer_peer_closed spends the one allowed queue round waiting for replies to
// land; otherwise FIN (if we are closing) or tear down.
inline close_action on_peer_fin(state& s, const conn_facts& f) noexcept {
    assert(s.read == read_mode::eof);
    if (s.phase == conn_phase::closed) return close_action::none;
    if (!f.wbuf_empty) return close_action::none;
    if (!is_closing(s) && f.pending > 0 && s.round != close_round::peer_closed) {
        s.round = close_round::peer_closed;
        return close_action::defer_peer_closed;
    }
    if (s.phase == conn_phase::closing) return close_action::send_fin;
    return close_action::close_graceful;
}

// shutdown(SHUT_WR) succeeded.
inline void on_fin_sent(state& s) noexcept {
    assert(s.phase == conn_phase::closing);
    s.phase = conn_phase::fin_sent;
}

// Teardown. True the first time only. Normalises everything a dead connection
// must not claim: no write interest, no round, no pause.
inline bool on_closed(state& s) noexcept {
    if (s.phase == conn_phase::closed) return false;
    s.phase = conn_phase::closed;
    s.write_armed = false;
    s.in_round = false;
    if (s.read == read_mode::paused) {
        s.read = read_mode::active;
        s.paused_at_ms = 0;
    }
    return true;
}

}  // namespace sm

}  // namespace conn_fsm_spec
