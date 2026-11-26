// ===== BEGIN GENERATED (tools/gen_conn_fsm.cpp; do not edit by hand) =====
// Source of truth: tools/conn_fsm_spec.h (the rules) -> tools/gen_conn_fsm.cpp
// (this text, tests/conn_fsm_cases.inc, docs/conn-fsm.md). Regenerate; do not edit.
//
// A state is one value of conn_state_id: a closed enum of the 77 legal
// combinations of (phase, read, in_round, round, write_armed). A combination
// the five rules forbid has no enumerator, so it cannot be named, let alone
// reached. The rules, from the 180 combinations of the five axes:
//   - round == close needs phase != open;
//   - round == peer_closed needs read == eof;
//   - read == paused needs phase open/draining and no serving round;
//   - closed has no write interest, no serving round and no pause;
//   - a serving round needs phase != open.
// Per phase: open 8, draining 24, closing 20, fin_sent 20, closed 5.
//
// <phase>_<read:act|pau|eof>[_<flags>]; flags in this order:
//   R serving round on the stack, C round=close, P round=peer_closed,
//   W write interest armed.
// Ids are ordered by (phase, read, in_round, round, write).
enum class conn_state_id : uint8_t {
    // open (8)
    open_act = 0,
    open_act_W = 1,
    open_pau = 2,
    open_pau_W = 3,
    open_eof = 4,
    open_eof_W = 5,
    open_eof_P = 6,
    open_eof_PW = 7,
    // draining (24)
    draining_act = 8,
    draining_act_W = 9,
    draining_act_C = 10,
    draining_act_CW = 11,
    draining_act_R = 12,
    draining_act_RW = 13,
    draining_act_RC = 14,
    draining_act_RCW = 15,
    draining_pau = 16,
    draining_pau_W = 17,
    draining_pau_C = 18,
    draining_pau_CW = 19,
    draining_eof = 20,
    draining_eof_W = 21,
    draining_eof_C = 22,
    draining_eof_CW = 23,
    draining_eof_P = 24,
    draining_eof_PW = 25,
    draining_eof_R = 26,
    draining_eof_RW = 27,
    draining_eof_RC = 28,
    draining_eof_RCW = 29,
    draining_eof_RP = 30,
    draining_eof_RPW = 31,
    // closing (20)
    closing_act = 32,
    closing_act_W = 33,
    closing_act_C = 34,
    closing_act_CW = 35,
    closing_act_R = 36,
    closing_act_RW = 37,
    closing_act_RC = 38,
    closing_act_RCW = 39,
    closing_eof = 40,
    closing_eof_W = 41,
    closing_eof_C = 42,
    closing_eof_CW = 43,
    closing_eof_P = 44,
    closing_eof_PW = 45,
    closing_eof_R = 46,
    closing_eof_RW = 47,
    closing_eof_RC = 48,
    closing_eof_RCW = 49,
    closing_eof_RP = 50,
    closing_eof_RPW = 51,
    // fin_sent (20)
    fin_sent_act = 52,
    fin_sent_act_W = 53,
    fin_sent_act_C = 54,
    fin_sent_act_CW = 55,
    fin_sent_act_R = 56,
    fin_sent_act_RW = 57,
    fin_sent_act_RC = 58,
    fin_sent_act_RCW = 59,
    fin_sent_eof = 60,
    fin_sent_eof_W = 61,
    fin_sent_eof_C = 62,
    fin_sent_eof_CW = 63,
    fin_sent_eof_P = 64,
    fin_sent_eof_PW = 65,
    fin_sent_eof_R = 66,
    fin_sent_eof_RW = 67,
    fin_sent_eof_RC = 68,
    fin_sent_eof_RCW = 69,
    fin_sent_eof_RP = 70,
    fin_sent_eof_RPW = 71,
    // closed (5)
    closed_act = 72,
    closed_act_C = 73,
    closed_eof = 74,
    closed_eof_C = 75,
    closed_eof_P = 76,
};
inline constexpr int k_conn_state_count = 77;

constexpr conn_phase phase_of(conn_state_id s) noexcept {
    using S = conn_state_id;
    switch (s) {
    case S::draining_act: case S::draining_act_W: case S::draining_act_C: 
    case S::draining_act_CW: case S::draining_act_R: case S::draining_act_RW: 
    case S::draining_act_RC: case S::draining_act_RCW: case S::draining_pau: 
    case S::draining_pau_W: case S::draining_pau_C: case S::draining_pau_CW: 
    case S::draining_eof: case S::draining_eof_W: case S::draining_eof_C: 
    case S::draining_eof_CW: case S::draining_eof_P: case S::draining_eof_PW: 
    case S::draining_eof_R: case S::draining_eof_RW: case S::draining_eof_RC: 
    case S::draining_eof_RCW: case S::draining_eof_RP: case S::draining_eof_RPW: 
        return conn_phase::draining;
    case S::closing_act: case S::closing_act_W: case S::closing_act_C: 
    case S::closing_act_CW: case S::closing_act_R: case S::closing_act_RW: 
    case S::closing_act_RC: case S::closing_act_RCW: case S::closing_eof: 
    case S::closing_eof_W: case S::closing_eof_C: case S::closing_eof_CW: 
    case S::closing_eof_P: case S::closing_eof_PW: case S::closing_eof_R: 
    case S::closing_eof_RW: case S::closing_eof_RC: case S::closing_eof_RCW: 
    case S::closing_eof_RP: case S::closing_eof_RPW: 
        return conn_phase::closing;
    case S::fin_sent_act: case S::fin_sent_act_W: case S::fin_sent_act_C: 
    case S::fin_sent_act_CW: case S::fin_sent_act_R: case S::fin_sent_act_RW: 
    case S::fin_sent_act_RC: case S::fin_sent_act_RCW: case S::fin_sent_eof: 
    case S::fin_sent_eof_W: case S::fin_sent_eof_C: case S::fin_sent_eof_CW: 
    case S::fin_sent_eof_P: case S::fin_sent_eof_PW: case S::fin_sent_eof_R: 
    case S::fin_sent_eof_RW: case S::fin_sent_eof_RC: case S::fin_sent_eof_RCW: 
    case S::fin_sent_eof_RP: case S::fin_sent_eof_RPW: 
        return conn_phase::fin_sent;
    case S::closed_act: case S::closed_act_C: case S::closed_eof: case S::closed_eof_C: 
    case S::closed_eof_P: 
        return conn_phase::closed;
    default: break;
    }
    return conn_phase::open;
}

constexpr read_mode read_of(conn_state_id s) noexcept {
    using S = conn_state_id;
    switch (s) {
    case S::open_pau: case S::open_pau_W: case S::draining_pau: case S::draining_pau_W: 
    case S::draining_pau_C: case S::draining_pau_CW: 
        return read_mode::paused;
    case S::open_eof: case S::open_eof_W: case S::open_eof_P: case S::open_eof_PW: 
    case S::draining_eof: case S::draining_eof_W: case S::draining_eof_C: 
    case S::draining_eof_CW: case S::draining_eof_P: case S::draining_eof_PW: 
    case S::draining_eof_R: case S::draining_eof_RW: case S::draining_eof_RC: 
    case S::draining_eof_RCW: case S::draining_eof_RP: case S::draining_eof_RPW: 
    case S::closing_eof: case S::closing_eof_W: case S::closing_eof_C: 
    case S::closing_eof_CW: case S::closing_eof_P: case S::closing_eof_PW: 
    case S::closing_eof_R: case S::closing_eof_RW: case S::closing_eof_RC: 
    case S::closing_eof_RCW: case S::closing_eof_RP: case S::closing_eof_RPW: 
    case S::fin_sent_eof: case S::fin_sent_eof_W: case S::fin_sent_eof_C: 
    case S::fin_sent_eof_CW: case S::fin_sent_eof_P: case S::fin_sent_eof_PW: 
    case S::fin_sent_eof_R: case S::fin_sent_eof_RW: case S::fin_sent_eof_RC: 
    case S::fin_sent_eof_RCW: case S::fin_sent_eof_RP: case S::fin_sent_eof_RPW: 
    case S::closed_eof: case S::closed_eof_C: case S::closed_eof_P: 
        return read_mode::eof;
    default: break;
    }
    return read_mode::active;
}

constexpr close_round round_of(conn_state_id s) noexcept {
    using S = conn_state_id;
    switch (s) {
    case S::draining_act_C: case S::draining_act_CW: case S::draining_act_RC: 
    case S::draining_act_RCW: case S::draining_pau_C: case S::draining_pau_CW: 
    case S::draining_eof_C: case S::draining_eof_CW: case S::draining_eof_RC: 
    case S::draining_eof_RCW: case S::closing_act_C: case S::closing_act_CW: 
    case S::closing_act_RC: case S::closing_act_RCW: case S::closing_eof_C: 
    case S::closing_eof_CW: case S::closing_eof_RC: case S::closing_eof_RCW: 
    case S::fin_sent_act_C: case S::fin_sent_act_CW: case S::fin_sent_act_RC: 
    case S::fin_sent_act_RCW: case S::fin_sent_eof_C: case S::fin_sent_eof_CW: 
    case S::fin_sent_eof_RC: case S::fin_sent_eof_RCW: case S::closed_act_C: 
    case S::closed_eof_C: 
        return close_round::close;
    case S::open_eof_P: case S::open_eof_PW: case S::draining_eof_P: 
    case S::draining_eof_PW: case S::draining_eof_RP: case S::draining_eof_RPW: 
    case S::closing_eof_P: case S::closing_eof_PW: case S::closing_eof_RP: 
    case S::closing_eof_RPW: case S::fin_sent_eof_P: case S::fin_sent_eof_PW: 
    case S::fin_sent_eof_RP: case S::fin_sent_eof_RPW: case S::closed_eof_P: 
        return close_round::peer_closed;
    default: break;
    }
    return close_round::none;
}

constexpr bool write_armed_of(conn_state_id s) noexcept {
    using S = conn_state_id;
    switch (s) {
    case S::open_act_W: case S::open_pau_W: case S::open_eof_W: case S::open_eof_PW: 
    case S::draining_act_W: case S::draining_act_CW: case S::draining_act_RW: 
    case S::draining_act_RCW: case S::draining_pau_W: case S::draining_pau_CW: 
    case S::draining_eof_W: case S::draining_eof_CW: case S::draining_eof_PW: 
    case S::draining_eof_RW: case S::draining_eof_RCW: case S::draining_eof_RPW: 
    case S::closing_act_W: case S::closing_act_CW: case S::closing_act_RW: 
    case S::closing_act_RCW: case S::closing_eof_W: case S::closing_eof_CW: 
    case S::closing_eof_PW: case S::closing_eof_RW: case S::closing_eof_RCW: 
    case S::closing_eof_RPW: case S::fin_sent_act_W: case S::fin_sent_act_CW: 
    case S::fin_sent_act_RW: case S::fin_sent_act_RCW: case S::fin_sent_eof_W: 
    case S::fin_sent_eof_CW: case S::fin_sent_eof_PW: case S::fin_sent_eof_RW: 
    case S::fin_sent_eof_RCW: case S::fin_sent_eof_RPW: 
        return true;
    default: break;
    }
    return false;
}

constexpr bool in_round_of(conn_state_id s) noexcept {
    using S = conn_state_id;
    switch (s) {
    case S::draining_act_R: case S::draining_act_RW: case S::draining_act_RC: 
    case S::draining_act_RCW: case S::draining_eof_R: case S::draining_eof_RW: 
    case S::draining_eof_RC: case S::draining_eof_RCW: case S::draining_eof_RP: 
    case S::draining_eof_RPW: case S::closing_act_R: case S::closing_act_RW: 
    case S::closing_act_RC: case S::closing_act_RCW: case S::closing_eof_R: 
    case S::closing_eof_RW: case S::closing_eof_RC: case S::closing_eof_RCW: 
    case S::closing_eof_RP: case S::closing_eof_RPW: case S::fin_sent_act_R: 
    case S::fin_sent_act_RW: case S::fin_sent_act_RC: case S::fin_sent_act_RCW: 
    case S::fin_sent_eof_R: case S::fin_sent_eof_RW: case S::fin_sent_eof_RC: 
    case S::fin_sent_eof_RCW: case S::fin_sent_eof_RP: case S::fin_sent_eof_RPW: 
        return true;
    default: break;
    }
    return false;
}

constexpr const char* name_of(conn_state_id s) noexcept {
    switch (s) {
    case conn_state_id::open_act: return "open_act";
    case conn_state_id::open_act_W: return "open_act_W";
    case conn_state_id::open_pau: return "open_pau";
    case conn_state_id::open_pau_W: return "open_pau_W";
    case conn_state_id::open_eof: return "open_eof";
    case conn_state_id::open_eof_W: return "open_eof_W";
    case conn_state_id::open_eof_P: return "open_eof_P";
    case conn_state_id::open_eof_PW: return "open_eof_PW";
    case conn_state_id::draining_act: return "draining_act";
    case conn_state_id::draining_act_W: return "draining_act_W";
    case conn_state_id::draining_act_C: return "draining_act_C";
    case conn_state_id::draining_act_CW: return "draining_act_CW";
    case conn_state_id::draining_act_R: return "draining_act_R";
    case conn_state_id::draining_act_RW: return "draining_act_RW";
    case conn_state_id::draining_act_RC: return "draining_act_RC";
    case conn_state_id::draining_act_RCW: return "draining_act_RCW";
    case conn_state_id::draining_pau: return "draining_pau";
    case conn_state_id::draining_pau_W: return "draining_pau_W";
    case conn_state_id::draining_pau_C: return "draining_pau_C";
    case conn_state_id::draining_pau_CW: return "draining_pau_CW";
    case conn_state_id::draining_eof: return "draining_eof";
    case conn_state_id::draining_eof_W: return "draining_eof_W";
    case conn_state_id::draining_eof_C: return "draining_eof_C";
    case conn_state_id::draining_eof_CW: return "draining_eof_CW";
    case conn_state_id::draining_eof_P: return "draining_eof_P";
    case conn_state_id::draining_eof_PW: return "draining_eof_PW";
    case conn_state_id::draining_eof_R: return "draining_eof_R";
    case conn_state_id::draining_eof_RW: return "draining_eof_RW";
    case conn_state_id::draining_eof_RC: return "draining_eof_RC";
    case conn_state_id::draining_eof_RCW: return "draining_eof_RCW";
    case conn_state_id::draining_eof_RP: return "draining_eof_RP";
    case conn_state_id::draining_eof_RPW: return "draining_eof_RPW";
    case conn_state_id::closing_act: return "closing_act";
    case conn_state_id::closing_act_W: return "closing_act_W";
    case conn_state_id::closing_act_C: return "closing_act_C";
    case conn_state_id::closing_act_CW: return "closing_act_CW";
    case conn_state_id::closing_act_R: return "closing_act_R";
    case conn_state_id::closing_act_RW: return "closing_act_RW";
    case conn_state_id::closing_act_RC: return "closing_act_RC";
    case conn_state_id::closing_act_RCW: return "closing_act_RCW";
    case conn_state_id::closing_eof: return "closing_eof";
    case conn_state_id::closing_eof_W: return "closing_eof_W";
    case conn_state_id::closing_eof_C: return "closing_eof_C";
    case conn_state_id::closing_eof_CW: return "closing_eof_CW";
    case conn_state_id::closing_eof_P: return "closing_eof_P";
    case conn_state_id::closing_eof_PW: return "closing_eof_PW";
    case conn_state_id::closing_eof_R: return "closing_eof_R";
    case conn_state_id::closing_eof_RW: return "closing_eof_RW";
    case conn_state_id::closing_eof_RC: return "closing_eof_RC";
    case conn_state_id::closing_eof_RCW: return "closing_eof_RCW";
    case conn_state_id::closing_eof_RP: return "closing_eof_RP";
    case conn_state_id::closing_eof_RPW: return "closing_eof_RPW";
    case conn_state_id::fin_sent_act: return "fin_sent_act";
    case conn_state_id::fin_sent_act_W: return "fin_sent_act_W";
    case conn_state_id::fin_sent_act_C: return "fin_sent_act_C";
    case conn_state_id::fin_sent_act_CW: return "fin_sent_act_CW";
    case conn_state_id::fin_sent_act_R: return "fin_sent_act_R";
    case conn_state_id::fin_sent_act_RW: return "fin_sent_act_RW";
    case conn_state_id::fin_sent_act_RC: return "fin_sent_act_RC";
    case conn_state_id::fin_sent_act_RCW: return "fin_sent_act_RCW";
    case conn_state_id::fin_sent_eof: return "fin_sent_eof";
    case conn_state_id::fin_sent_eof_W: return "fin_sent_eof_W";
    case conn_state_id::fin_sent_eof_C: return "fin_sent_eof_C";
    case conn_state_id::fin_sent_eof_CW: return "fin_sent_eof_CW";
    case conn_state_id::fin_sent_eof_P: return "fin_sent_eof_P";
    case conn_state_id::fin_sent_eof_PW: return "fin_sent_eof_PW";
    case conn_state_id::fin_sent_eof_R: return "fin_sent_eof_R";
    case conn_state_id::fin_sent_eof_RW: return "fin_sent_eof_RW";
    case conn_state_id::fin_sent_eof_RC: return "fin_sent_eof_RC";
    case conn_state_id::fin_sent_eof_RCW: return "fin_sent_eof_RCW";
    case conn_state_id::fin_sent_eof_RP: return "fin_sent_eof_RP";
    case conn_state_id::fin_sent_eof_RPW: return "fin_sent_eof_RPW";
    case conn_state_id::closed_act: return "closed_act";
    case conn_state_id::closed_act_C: return "closed_act_C";
    case conn_state_id::closed_eof: return "closed_eof";
    case conn_state_id::closed_eof_C: return "closed_eof_C";
    case conn_state_id::closed_eof_P: return "closed_eof_P";
    }
    return "?";
}

// Events. Each is a function of the state and, where the decision reads the
// connection's buffers, of one guard.
enum class conn_event : uint8_t {
    pause,
    resume,
    eof,
    write_on,
    write_off,
    graceful,
    request,
    round_begin,
    round_end,
    input_drained,
    input_defer_failed,
    peer_fin,
    fin_sent,
    closed,
};

// The facts an event branches on, reduced to the classes its decision
// distinguishes. The reduction is lossless (the generator checks the oracle
// against it over every combination of the facts).
//   input_drained (a serving round finished):
//     more       served > 0, rbuf still holds input, budget left
//     served     served > 0 otherwise
//     idle_owed  served == 0, replies are owed (pending > 0)
//     idle_clear served == 0, nothing owed
//   peer_fin (the peer's FIN, wbuf permitting):
//     wbuf_busy  wbuf holds bytes: not our turn yet
//     owed       wbuf empty, replies still reserved
//     clear      wbuf empty, nothing owed
enum class guard : uint8_t {
    none,
    more, served, idle_owed, idle_clear,
    wbuf_busy, owed, clear,
};

inline constexpr guard input_guard(bool served, bool rbuf_nonempty, bool budget_left,
                                   bool pending) noexcept {
    if (served) return (rbuf_nonempty && budget_left) ? guard::more : guard::served;
    return pending ? guard::idle_owed : guard::idle_clear;
}
inline constexpr guard peer_guard(bool wbuf_empty, bool pending) noexcept {
    if (!wbuf_empty) return guard::wbuf_busy;
    return pending ? guard::owed : guard::clear;
}

// Payload effects of a transition, applied by the wrapper that owns the
// payloads (the table decides *whether*, the payload decides *what*).
namespace conn_fx {
constexpr uint8_t stamp = 1;    // the state's stamp := now
constexpr uint8_t measure = 2;  // an unmeasured budget := bytes buffered in rbuf
constexpr uint8_t charge = 4;   // budget := max(0, budget - dealt)
constexpr uint8_t resume = 8;   // reads were resumed: report it to the reactor
}  // namespace conn_fx

struct conn_step {
    conn_state_id next;
    close_action  action;
    uint8_t       fx;
};

// (state, event, guard) -> (next, action, effects). Cells not listed leave the
// state alone and do nothing; an event issued outside its precondition (see
// conn_machine) is such a cell. An inline switch, not a data array: every
// translation unit includes this header.
constexpr conn_step transition(conn_state_id s, conn_event e, guard g) noexcept {
    using S = conn_state_id;
    using A = close_action;
    (void)g;
    switch (e) {
    case conn_event::pause:
        switch (s) {
        case S::open_act: return {S::open_pau, A::none, conn_fx::stamp};
        case S::open_act_W: return {S::open_pau_W, A::none, conn_fx::stamp};
        case S::draining_act: return {S::draining_pau, A::none, conn_fx::stamp};
        case S::draining_act_W: return {S::draining_pau_W, A::none, conn_fx::stamp};
        case S::draining_act_C: return {S::draining_pau_C, A::none, conn_fx::stamp};
        case S::draining_act_CW: return {S::draining_pau_CW, A::none, conn_fx::stamp};
        default: break;
        }
        break;
    case conn_event::resume:
        switch (s) {
        case S::open_pau: return {S::open_act, A::none, conn_fx::resume};
        case S::open_pau_W: return {S::open_act_W, A::none, conn_fx::resume};
        case S::draining_pau: return {S::draining_act, A::none, conn_fx::resume};
        case S::draining_pau_W: return {S::draining_act_W, A::none, conn_fx::resume};
        case S::draining_pau_C: return {S::draining_act_C, A::none, conn_fx::resume};
        case S::draining_pau_CW: return {S::draining_act_CW, A::none, conn_fx::resume};
        default: break;
        }
        break;
    case conn_event::eof:
        switch (s) {
        case S::open_act: return {S::open_eof, A::none, 0};
        case S::open_act_W: return {S::open_eof_W, A::none, 0};
        case S::open_pau: return {S::open_eof, A::none, 0};
        case S::open_pau_W: return {S::open_eof_W, A::none, 0};
        case S::draining_act: return {S::draining_eof, A::none, 0};
        case S::draining_act_W: return {S::draining_eof_W, A::none, 0};
        case S::draining_act_C: return {S::draining_eof_C, A::none, 0};
        case S::draining_act_CW: return {S::draining_eof_CW, A::none, 0};
        case S::draining_act_R: return {S::draining_eof_R, A::none, 0};
        case S::draining_act_RW: return {S::draining_eof_RW, A::none, 0};
        case S::draining_act_RC: return {S::draining_eof_RC, A::none, 0};
        case S::draining_act_RCW: return {S::draining_eof_RCW, A::none, 0};
        case S::draining_pau: return {S::draining_eof, A::none, 0};
        case S::draining_pau_W: return {S::draining_eof_W, A::none, 0};
        case S::draining_pau_C: return {S::draining_eof_C, A::none, 0};
        case S::draining_pau_CW: return {S::draining_eof_CW, A::none, 0};
        case S::closing_act: return {S::closing_eof, A::none, 0};
        case S::closing_act_W: return {S::closing_eof_W, A::none, 0};
        case S::closing_act_C: return {S::closing_eof_C, A::none, 0};
        case S::closing_act_CW: return {S::closing_eof_CW, A::none, 0};
        case S::closing_act_R: return {S::closing_eof_R, A::none, 0};
        case S::closing_act_RW: return {S::closing_eof_RW, A::none, 0};
        case S::closing_act_RC: return {S::closing_eof_RC, A::none, 0};
        case S::closing_act_RCW: return {S::closing_eof_RCW, A::none, 0};
        case S::fin_sent_act: return {S::fin_sent_eof, A::none, 0};
        case S::fin_sent_act_W: return {S::fin_sent_eof_W, A::none, 0};
        case S::fin_sent_act_C: return {S::fin_sent_eof_C, A::none, 0};
        case S::fin_sent_act_CW: return {S::fin_sent_eof_CW, A::none, 0};
        case S::fin_sent_act_R: return {S::fin_sent_eof_R, A::none, 0};
        case S::fin_sent_act_RW: return {S::fin_sent_eof_RW, A::none, 0};
        case S::fin_sent_act_RC: return {S::fin_sent_eof_RC, A::none, 0};
        case S::fin_sent_act_RCW: return {S::fin_sent_eof_RCW, A::none, 0};
        case S::closed_act: return {S::closed_eof, A::none, 0};
        case S::closed_act_C: return {S::closed_eof_C, A::none, 0};
        default: break;
        }
        break;
    case conn_event::write_on:
        switch (s) {
        case S::open_act: return {S::open_act_W, A::none, 0};
        case S::open_pau: return {S::open_pau_W, A::none, 0};
        case S::open_eof: return {S::open_eof_W, A::none, 0};
        case S::open_eof_P: return {S::open_eof_PW, A::none, 0};
        case S::draining_act: return {S::draining_act_W, A::none, 0};
        case S::draining_act_C: return {S::draining_act_CW, A::none, 0};
        case S::draining_act_R: return {S::draining_act_RW, A::none, 0};
        case S::draining_act_RC: return {S::draining_act_RCW, A::none, 0};
        case S::draining_pau: return {S::draining_pau_W, A::none, 0};
        case S::draining_pau_C: return {S::draining_pau_CW, A::none, 0};
        case S::draining_eof: return {S::draining_eof_W, A::none, 0};
        case S::draining_eof_C: return {S::draining_eof_CW, A::none, 0};
        case S::draining_eof_P: return {S::draining_eof_PW, A::none, 0};
        case S::draining_eof_R: return {S::draining_eof_RW, A::none, 0};
        case S::draining_eof_RC: return {S::draining_eof_RCW, A::none, 0};
        case S::draining_eof_RP: return {S::draining_eof_RPW, A::none, 0};
        case S::closing_act: return {S::closing_act_W, A::none, 0};
        case S::closing_act_C: return {S::closing_act_CW, A::none, 0};
        case S::closing_act_R: return {S::closing_act_RW, A::none, 0};
        case S::closing_act_RC: return {S::closing_act_RCW, A::none, 0};
        case S::closing_eof: return {S::closing_eof_W, A::none, 0};
        case S::closing_eof_C: return {S::closing_eof_CW, A::none, 0};
        case S::closing_eof_P: return {S::closing_eof_PW, A::none, 0};
        case S::closing_eof_R: return {S::closing_eof_RW, A::none, 0};
        case S::closing_eof_RC: return {S::closing_eof_RCW, A::none, 0};
        case S::closing_eof_RP: return {S::closing_eof_RPW, A::none, 0};
        case S::fin_sent_act: return {S::fin_sent_act_W, A::none, 0};
        case S::fin_sent_act_C: return {S::fin_sent_act_CW, A::none, 0};
        case S::fin_sent_act_R: return {S::fin_sent_act_RW, A::none, 0};
        case S::fin_sent_act_RC: return {S::fin_sent_act_RCW, A::none, 0};
        case S::fin_sent_eof: return {S::fin_sent_eof_W, A::none, 0};
        case S::fin_sent_eof_C: return {S::fin_sent_eof_CW, A::none, 0};
        case S::fin_sent_eof_P: return {S::fin_sent_eof_PW, A::none, 0};
        case S::fin_sent_eof_R: return {S::fin_sent_eof_RW, A::none, 0};
        case S::fin_sent_eof_RC: return {S::fin_sent_eof_RCW, A::none, 0};
        case S::fin_sent_eof_RP: return {S::fin_sent_eof_RPW, A::none, 0};
        default: break;
        }
        break;
    case conn_event::write_off:
        switch (s) {
        case S::open_act_W: return {S::open_act, A::none, 0};
        case S::open_pau_W: return {S::open_pau, A::none, 0};
        case S::open_eof_W: return {S::open_eof, A::none, 0};
        case S::open_eof_PW: return {S::open_eof_P, A::none, 0};
        case S::draining_act_W: return {S::draining_act, A::none, 0};
        case S::draining_act_CW: return {S::draining_act_C, A::none, 0};
        case S::draining_act_RW: return {S::draining_act_R, A::none, 0};
        case S::draining_act_RCW: return {S::draining_act_RC, A::none, 0};
        case S::draining_pau_W: return {S::draining_pau, A::none, 0};
        case S::draining_pau_CW: return {S::draining_pau_C, A::none, 0};
        case S::draining_eof_W: return {S::draining_eof, A::none, 0};
        case S::draining_eof_CW: return {S::draining_eof_C, A::none, 0};
        case S::draining_eof_PW: return {S::draining_eof_P, A::none, 0};
        case S::draining_eof_RW: return {S::draining_eof_R, A::none, 0};
        case S::draining_eof_RCW: return {S::draining_eof_RC, A::none, 0};
        case S::draining_eof_RPW: return {S::draining_eof_RP, A::none, 0};
        case S::closing_act_W: return {S::closing_act, A::none, 0};
        case S::closing_act_CW: return {S::closing_act_C, A::none, 0};
        case S::closing_act_RW: return {S::closing_act_R, A::none, 0};
        case S::closing_act_RCW: return {S::closing_act_RC, A::none, 0};
        case S::closing_eof_W: return {S::closing_eof, A::none, 0};
        case S::closing_eof_CW: return {S::closing_eof_C, A::none, 0};
        case S::closing_eof_PW: return {S::closing_eof_P, A::none, 0};
        case S::closing_eof_RW: return {S::closing_eof_R, A::none, 0};
        case S::closing_eof_RCW: return {S::closing_eof_RC, A::none, 0};
        case S::closing_eof_RPW: return {S::closing_eof_RP, A::none, 0};
        case S::fin_sent_act_W: return {S::fin_sent_act, A::none, 0};
        case S::fin_sent_act_CW: return {S::fin_sent_act_C, A::none, 0};
        case S::fin_sent_act_RW: return {S::fin_sent_act_R, A::none, 0};
        case S::fin_sent_act_RCW: return {S::fin_sent_act_RC, A::none, 0};
        case S::fin_sent_eof_W: return {S::fin_sent_eof, A::none, 0};
        case S::fin_sent_eof_CW: return {S::fin_sent_eof_C, A::none, 0};
        case S::fin_sent_eof_PW: return {S::fin_sent_eof_P, A::none, 0};
        case S::fin_sent_eof_RW: return {S::fin_sent_eof_R, A::none, 0};
        case S::fin_sent_eof_RCW: return {S::fin_sent_eof_RC, A::none, 0};
        case S::fin_sent_eof_RPW: return {S::fin_sent_eof_RP, A::none, 0};
        default: break;
        }
        break;
    case conn_event::graceful:
        switch (s) {
        case S::open_act: return {S::closing_act, A::drain_wbuf, conn_fx::stamp};
        case S::open_act_W: return {S::closing_act_W, A::drain_wbuf, conn_fx::stamp};
        case S::open_pau: return {S::closing_act, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::open_pau_W: return {S::closing_act_W, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::open_eof: return {S::closing_eof, A::drain_wbuf, conn_fx::stamp};
        case S::open_eof_W: return {S::closing_eof_W, A::drain_wbuf, conn_fx::stamp};
        case S::open_eof_P: return {S::closing_eof_P, A::drain_wbuf, conn_fx::stamp};
        case S::open_eof_PW: return {S::closing_eof_PW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act: return {S::closing_act, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_W: return {S::closing_act_W, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_C: return {S::closing_act_C, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_CW: return {S::closing_act_CW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_R: return {S::closing_act_R, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_RW: return {S::closing_act_RW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_RC: return {S::closing_act_RC, A::drain_wbuf, conn_fx::stamp};
        case S::draining_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_pau: return {S::closing_act, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::draining_pau_W: return {S::closing_act_W, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::draining_pau_C: return {S::closing_act_C, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::draining_pau_CW: return {S::closing_act_CW, A::drain_wbuf, conn_fx::stamp | conn_fx::resume};
        case S::draining_eof: return {S::closing_eof, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_W: return {S::closing_eof_W, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_C: return {S::closing_eof_C, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_P: return {S::closing_eof_P, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_PW: return {S::closing_eof_PW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_R: return {S::closing_eof_R, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_RW: return {S::closing_eof_RW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_RP: return {S::closing_eof_RP, A::drain_wbuf, conn_fx::stamp};
        case S::draining_eof_RPW: return {S::closing_eof_RPW, A::drain_wbuf, conn_fx::stamp};
        case S::closing_act: return {S::closing_act, A::drain_wbuf, 0};
        case S::closing_act_W: return {S::closing_act_W, A::drain_wbuf, 0};
        case S::closing_act_C: return {S::closing_act_C, A::drain_wbuf, 0};
        case S::closing_act_CW: return {S::closing_act_CW, A::drain_wbuf, 0};
        case S::closing_act_R: return {S::closing_act_R, A::drain_wbuf, 0};
        case S::closing_act_RW: return {S::closing_act_RW, A::drain_wbuf, 0};
        case S::closing_act_RC: return {S::closing_act_RC, A::drain_wbuf, 0};
        case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, 0};
        case S::closing_eof: return {S::closing_eof, A::drain_wbuf, 0};
        case S::closing_eof_W: return {S::closing_eof_W, A::drain_wbuf, 0};
        case S::closing_eof_C: return {S::closing_eof_C, A::drain_wbuf, 0};
        case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, 0};
        case S::closing_eof_P: return {S::closing_eof_P, A::drain_wbuf, 0};
        case S::closing_eof_PW: return {S::closing_eof_PW, A::drain_wbuf, 0};
        case S::closing_eof_R: return {S::closing_eof_R, A::drain_wbuf, 0};
        case S::closing_eof_RW: return {S::closing_eof_RW, A::drain_wbuf, 0};
        case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, 0};
        case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, 0};
        case S::closing_eof_RP: return {S::closing_eof_RP, A::drain_wbuf, 0};
        case S::closing_eof_RPW: return {S::closing_eof_RPW, A::drain_wbuf, 0};
        default: break;
        }
        break;
    case conn_event::request:
        switch (s) {
        case S::open_act: return {S::draining_act, A::drain_input, conn_fx::measure};
        case S::open_act_W: return {S::draining_act_W, A::drain_input, conn_fx::measure};
        case S::open_pau: return {S::draining_act, A::drain_input, conn_fx::measure | conn_fx::resume};
        case S::open_pau_W: return {S::draining_act_W, A::drain_input, conn_fx::measure | conn_fx::resume};
        case S::open_eof: return {S::draining_eof, A::drain_input, conn_fx::measure};
        case S::open_eof_W: return {S::draining_eof_W, A::drain_input, conn_fx::measure};
        case S::open_eof_P: return {S::draining_eof_P, A::drain_input, conn_fx::measure};
        case S::open_eof_PW: return {S::draining_eof_PW, A::drain_input, conn_fx::measure};
        case S::draining_act: return {S::draining_act, A::drain_input, 0};
        case S::draining_act_W: return {S::draining_act_W, A::drain_input, 0};
        case S::draining_act_C: return {S::draining_act_C, A::drain_input, 0};
        case S::draining_act_CW: return {S::draining_act_CW, A::drain_input, 0};
        case S::draining_act_R: return {S::draining_act_R, A::drain_input, 0};
        case S::draining_act_RW: return {S::draining_act_RW, A::drain_input, 0};
        case S::draining_act_RC: return {S::draining_act_RC, A::drain_input, 0};
        case S::draining_act_RCW: return {S::draining_act_RCW, A::drain_input, 0};
        case S::draining_pau: return {S::draining_act, A::drain_input, conn_fx::resume};
        case S::draining_pau_W: return {S::draining_act_W, A::drain_input, conn_fx::resume};
        case S::draining_pau_C: return {S::draining_act_C, A::drain_input, conn_fx::resume};
        case S::draining_pau_CW: return {S::draining_act_CW, A::drain_input, conn_fx::resume};
        case S::draining_eof: return {S::draining_eof, A::drain_input, 0};
        case S::draining_eof_W: return {S::draining_eof_W, A::drain_input, 0};
        case S::draining_eof_C: return {S::draining_eof_C, A::drain_input, 0};
        case S::draining_eof_CW: return {S::draining_eof_CW, A::drain_input, 0};
        case S::draining_eof_P: return {S::draining_eof_P, A::drain_input, 0};
        case S::draining_eof_PW: return {S::draining_eof_PW, A::drain_input, 0};
        case S::draining_eof_R: return {S::draining_eof_R, A::drain_input, 0};
        case S::draining_eof_RW: return {S::draining_eof_RW, A::drain_input, 0};
        case S::draining_eof_RC: return {S::draining_eof_RC, A::drain_input, 0};
        case S::draining_eof_RCW: return {S::draining_eof_RCW, A::drain_input, 0};
        case S::draining_eof_RP: return {S::draining_eof_RP, A::drain_input, 0};
        case S::draining_eof_RPW: return {S::draining_eof_RPW, A::drain_input, 0};
        case S::closing_act: return {S::closing_act, A::drain_input, conn_fx::measure};
        case S::closing_act_W: return {S::closing_act_W, A::drain_input, conn_fx::measure};
        case S::closing_act_C: return {S::closing_act_C, A::drain_input, conn_fx::measure};
        case S::closing_act_CW: return {S::closing_act_CW, A::drain_input, conn_fx::measure};
        case S::closing_act_R: return {S::closing_act_R, A::drain_input, conn_fx::measure};
        case S::closing_act_RW: return {S::closing_act_RW, A::drain_input, conn_fx::measure};
        case S::closing_act_RC: return {S::closing_act_RC, A::drain_input, conn_fx::measure};
        case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_input, conn_fx::measure};
        case S::closing_eof: return {S::closing_eof, A::drain_input, conn_fx::measure};
        case S::closing_eof_W: return {S::closing_eof_W, A::drain_input, conn_fx::measure};
        case S::closing_eof_C: return {S::closing_eof_C, A::drain_input, conn_fx::measure};
        case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_input, conn_fx::measure};
        case S::closing_eof_P: return {S::closing_eof_P, A::drain_input, conn_fx::measure};
        case S::closing_eof_PW: return {S::closing_eof_PW, A::drain_input, conn_fx::measure};
        case S::closing_eof_R: return {S::closing_eof_R, A::drain_input, conn_fx::measure};
        case S::closing_eof_RW: return {S::closing_eof_RW, A::drain_input, conn_fx::measure};
        case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_input, conn_fx::measure};
        case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_input, conn_fx::measure};
        case S::closing_eof_RP: return {S::closing_eof_RP, A::drain_input, conn_fx::measure};
        case S::closing_eof_RPW: return {S::closing_eof_RPW, A::drain_input, conn_fx::measure};
        default: break;
        }
        break;
    case conn_event::round_begin:
        switch (s) {
        case S::draining_act: return {S::draining_act_R, A::none, 0};
        case S::draining_act_W: return {S::draining_act_RW, A::none, 0};
        case S::draining_act_C: return {S::draining_act_RC, A::none, 0};
        case S::draining_act_CW: return {S::draining_act_RCW, A::none, 0};
        case S::draining_eof: return {S::draining_eof_R, A::none, 0};
        case S::draining_eof_W: return {S::draining_eof_RW, A::none, 0};
        case S::draining_eof_C: return {S::draining_eof_RC, A::none, 0};
        case S::draining_eof_CW: return {S::draining_eof_RCW, A::none, 0};
        case S::draining_eof_P: return {S::draining_eof_RP, A::none, 0};
        case S::draining_eof_PW: return {S::draining_eof_RPW, A::none, 0};
        case S::closing_act: return {S::closing_act_R, A::none, 0};
        case S::closing_act_W: return {S::closing_act_RW, A::none, 0};
        case S::closing_act_C: return {S::closing_act_RC, A::none, 0};
        case S::closing_act_CW: return {S::closing_act_RCW, A::none, 0};
        case S::closing_eof: return {S::closing_eof_R, A::none, 0};
        case S::closing_eof_W: return {S::closing_eof_RW, A::none, 0};
        case S::closing_eof_C: return {S::closing_eof_RC, A::none, 0};
        case S::closing_eof_CW: return {S::closing_eof_RCW, A::none, 0};
        case S::closing_eof_P: return {S::closing_eof_RP, A::none, 0};
        case S::closing_eof_PW: return {S::closing_eof_RPW, A::none, 0};
        default: break;
        }
        break;
    case conn_event::round_end:
        switch (s) {
        case S::draining_act_R: return {S::draining_act, A::none, 0};
        case S::draining_act_RW: return {S::draining_act_W, A::none, 0};
        case S::draining_act_RC: return {S::draining_act_C, A::none, 0};
        case S::draining_act_RCW: return {S::draining_act_CW, A::none, 0};
        case S::draining_eof_R: return {S::draining_eof, A::none, 0};
        case S::draining_eof_RW: return {S::draining_eof_W, A::none, 0};
        case S::draining_eof_RC: return {S::draining_eof_C, A::none, 0};
        case S::draining_eof_RCW: return {S::draining_eof_CW, A::none, 0};
        case S::draining_eof_RP: return {S::draining_eof_P, A::none, 0};
        case S::draining_eof_RPW: return {S::draining_eof_PW, A::none, 0};
        case S::closing_act_R: return {S::closing_act, A::none, 0};
        case S::closing_act_RW: return {S::closing_act_W, A::none, 0};
        case S::closing_act_RC: return {S::closing_act_C, A::none, 0};
        case S::closing_act_RCW: return {S::closing_act_CW, A::none, 0};
        case S::closing_eof_R: return {S::closing_eof, A::none, 0};
        case S::closing_eof_RW: return {S::closing_eof_W, A::none, 0};
        case S::closing_eof_RC: return {S::closing_eof_C, A::none, 0};
        case S::closing_eof_RCW: return {S::closing_eof_CW, A::none, 0};
        case S::closing_eof_RP: return {S::closing_eof_P, A::none, 0};
        case S::closing_eof_RPW: return {S::closing_eof_PW, A::none, 0};
        case S::fin_sent_act_R: return {S::fin_sent_act, A::none, 0};
        case S::fin_sent_act_RW: return {S::fin_sent_act_W, A::none, 0};
        case S::fin_sent_act_RC: return {S::fin_sent_act_C, A::none, 0};
        case S::fin_sent_act_RCW: return {S::fin_sent_act_CW, A::none, 0};
        case S::fin_sent_eof_R: return {S::fin_sent_eof, A::none, 0};
        case S::fin_sent_eof_RW: return {S::fin_sent_eof_W, A::none, 0};
        case S::fin_sent_eof_RC: return {S::fin_sent_eof_C, A::none, 0};
        case S::fin_sent_eof_RCW: return {S::fin_sent_eof_CW, A::none, 0};
        case S::fin_sent_eof_RP: return {S::fin_sent_eof_P, A::none, 0};
        case S::fin_sent_eof_RPW: return {S::fin_sent_eof_PW, A::none, 0};
        default: break;
        }
        break;
    case conn_event::input_drained:
        switch (g) {
        case guard::more:
            switch (s) {
            case S::draining_act: return {S::draining_act, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_W: return {S::draining_act_W, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_C: return {S::draining_act_C, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_CW: return {S::draining_act_CW, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_R: return {S::draining_act_R, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_RW: return {S::draining_act_RW, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_RC: return {S::draining_act_RC, A::defer_drain_input, conn_fx::charge};
            case S::draining_act_RCW: return {S::draining_act_RCW, A::defer_drain_input, conn_fx::charge};
            case S::draining_pau: return {S::draining_pau, A::defer_drain_input, conn_fx::charge};
            case S::draining_pau_W: return {S::draining_pau_W, A::defer_drain_input, conn_fx::charge};
            case S::draining_pau_C: return {S::draining_pau_C, A::defer_drain_input, conn_fx::charge};
            case S::draining_pau_CW: return {S::draining_pau_CW, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof: return {S::draining_eof, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_W: return {S::draining_eof_W, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_C: return {S::draining_eof_C, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_CW: return {S::draining_eof_CW, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_P: return {S::draining_eof_P, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_PW: return {S::draining_eof_PW, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_R: return {S::draining_eof_R, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_RW: return {S::draining_eof_RW, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_RC: return {S::draining_eof_RC, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_RCW: return {S::draining_eof_RCW, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_RP: return {S::draining_eof_RP, A::defer_drain_input, conn_fx::charge};
            case S::draining_eof_RPW: return {S::draining_eof_RPW, A::defer_drain_input, conn_fx::charge};
            case S::closing_act: return {S::closing_act, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_W: return {S::closing_act_W, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_C: return {S::closing_act_C, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_CW: return {S::closing_act_CW, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_R: return {S::closing_act_R, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_RW: return {S::closing_act_RW, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_RC: return {S::closing_act_RC, A::defer_drain_input, conn_fx::charge};
            case S::closing_act_RCW: return {S::closing_act_RCW, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof: return {S::closing_eof, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_W: return {S::closing_eof_W, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_C: return {S::closing_eof_C, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_P: return {S::closing_eof_P, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_PW: return {S::closing_eof_PW, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_R: return {S::closing_eof_R, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_RW: return {S::closing_eof_RW, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_RP: return {S::closing_eof_RP, A::defer_drain_input, conn_fx::charge};
            case S::closing_eof_RPW: return {S::closing_eof_RPW, A::defer_drain_input, conn_fx::charge};
            default: break;
            }
            break;
        case guard::served:
            switch (s) {
            case S::draining_act: return {S::draining_act_C, A::defer_park, conn_fx::charge};
            case S::draining_act_W: return {S::draining_act_CW, A::defer_park, conn_fx::charge};
            case S::draining_act_C: return {S::draining_act_C, A::drain_wbuf, conn_fx::charge};
            case S::draining_act_CW: return {S::draining_act_CW, A::drain_wbuf, conn_fx::charge};
            case S::draining_act_R: return {S::draining_act_RC, A::defer_park, conn_fx::charge};
            case S::draining_act_RW: return {S::draining_act_RCW, A::defer_park, conn_fx::charge};
            case S::draining_act_RC: return {S::draining_act_RC, A::drain_wbuf, conn_fx::charge};
            case S::draining_act_RCW: return {S::draining_act_RCW, A::drain_wbuf, conn_fx::charge};
            case S::draining_pau: return {S::draining_pau_C, A::defer_park, conn_fx::charge};
            case S::draining_pau_W: return {S::draining_pau_CW, A::defer_park, conn_fx::charge};
            case S::draining_pau_C: return {S::draining_pau_C, A::drain_wbuf, conn_fx::charge};
            case S::draining_pau_CW: return {S::draining_pau_CW, A::drain_wbuf, conn_fx::charge};
            case S::draining_eof: return {S::draining_eof_C, A::defer_park, conn_fx::charge};
            case S::draining_eof_W: return {S::draining_eof_CW, A::defer_park, conn_fx::charge};
            case S::draining_eof_C: return {S::draining_eof_C, A::drain_wbuf, conn_fx::charge};
            case S::draining_eof_CW: return {S::draining_eof_CW, A::drain_wbuf, conn_fx::charge};
            case S::draining_eof_P: return {S::draining_eof_P, A::none, conn_fx::charge};
            case S::draining_eof_PW: return {S::draining_eof_PW, A::none, conn_fx::charge};
            case S::draining_eof_R: return {S::draining_eof_RC, A::defer_park, conn_fx::charge};
            case S::draining_eof_RW: return {S::draining_eof_RCW, A::defer_park, conn_fx::charge};
            case S::draining_eof_RC: return {S::draining_eof_RC, A::drain_wbuf, conn_fx::charge};
            case S::draining_eof_RCW: return {S::draining_eof_RCW, A::drain_wbuf, conn_fx::charge};
            case S::draining_eof_RP: return {S::draining_eof_RP, A::none, conn_fx::charge};
            case S::draining_eof_RPW: return {S::draining_eof_RPW, A::none, conn_fx::charge};
            case S::closing_act: return {S::closing_act_C, A::defer_park, conn_fx::charge};
            case S::closing_act_W: return {S::closing_act_CW, A::defer_park, conn_fx::charge};
            case S::closing_act_C: return {S::closing_act_C, A::drain_wbuf, conn_fx::charge};
            case S::closing_act_CW: return {S::closing_act_CW, A::drain_wbuf, conn_fx::charge};
            case S::closing_act_R: return {S::closing_act_RC, A::defer_park, conn_fx::charge};
            case S::closing_act_RW: return {S::closing_act_RCW, A::defer_park, conn_fx::charge};
            case S::closing_act_RC: return {S::closing_act_RC, A::drain_wbuf, conn_fx::charge};
            case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, conn_fx::charge};
            case S::closing_eof: return {S::closing_eof_C, A::defer_park, conn_fx::charge};
            case S::closing_eof_W: return {S::closing_eof_CW, A::defer_park, conn_fx::charge};
            case S::closing_eof_C: return {S::closing_eof_C, A::drain_wbuf, conn_fx::charge};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, conn_fx::charge};
            case S::closing_eof_P: return {S::closing_eof_P, A::none, conn_fx::charge};
            case S::closing_eof_PW: return {S::closing_eof_PW, A::none, conn_fx::charge};
            case S::closing_eof_R: return {S::closing_eof_RC, A::defer_park, conn_fx::charge};
            case S::closing_eof_RW: return {S::closing_eof_RCW, A::defer_park, conn_fx::charge};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, conn_fx::charge};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, conn_fx::charge};
            case S::closing_eof_RP: return {S::closing_eof_RP, A::none, conn_fx::charge};
            case S::closing_eof_RPW: return {S::closing_eof_RPW, A::none, conn_fx::charge};
            default: break;
            }
            break;
        case guard::idle_owed:
            switch (s) {
            case S::draining_act: return {S::draining_act_C, A::defer_park, 0};
            case S::draining_act_W: return {S::draining_act_CW, A::defer_park, 0};
            case S::draining_act_C: return {S::draining_act_C, A::drain_wbuf, 0};
            case S::draining_act_CW: return {S::draining_act_CW, A::drain_wbuf, 0};
            case S::draining_act_R: return {S::draining_act_RC, A::defer_park, 0};
            case S::draining_act_RW: return {S::draining_act_RCW, A::defer_park, 0};
            case S::draining_act_RC: return {S::draining_act_RC, A::drain_wbuf, 0};
            case S::draining_act_RCW: return {S::draining_act_RCW, A::drain_wbuf, 0};
            case S::draining_pau: return {S::draining_pau_C, A::defer_park, 0};
            case S::draining_pau_W: return {S::draining_pau_CW, A::defer_park, 0};
            case S::draining_pau_C: return {S::draining_pau_C, A::drain_wbuf, 0};
            case S::draining_pau_CW: return {S::draining_pau_CW, A::drain_wbuf, 0};
            case S::draining_eof: return {S::draining_eof_C, A::defer_park, 0};
            case S::draining_eof_W: return {S::draining_eof_CW, A::defer_park, 0};
            case S::draining_eof_C: return {S::draining_eof_C, A::drain_wbuf, 0};
            case S::draining_eof_CW: return {S::draining_eof_CW, A::drain_wbuf, 0};
            case S::draining_eof_R: return {S::draining_eof_RC, A::defer_park, 0};
            case S::draining_eof_RW: return {S::draining_eof_RCW, A::defer_park, 0};
            case S::draining_eof_RC: return {S::draining_eof_RC, A::drain_wbuf, 0};
            case S::draining_eof_RCW: return {S::draining_eof_RCW, A::drain_wbuf, 0};
            case S::closing_act: return {S::closing_act_C, A::defer_park, 0};
            case S::closing_act_W: return {S::closing_act_CW, A::defer_park, 0};
            case S::closing_act_C: return {S::closing_act_C, A::drain_wbuf, 0};
            case S::closing_act_CW: return {S::closing_act_CW, A::drain_wbuf, 0};
            case S::closing_act_R: return {S::closing_act_RC, A::defer_park, 0};
            case S::closing_act_RW: return {S::closing_act_RCW, A::defer_park, 0};
            case S::closing_act_RC: return {S::closing_act_RC, A::drain_wbuf, 0};
            case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, 0};
            case S::closing_eof: return {S::closing_eof_C, A::defer_park, 0};
            case S::closing_eof_W: return {S::closing_eof_CW, A::defer_park, 0};
            case S::closing_eof_C: return {S::closing_eof_C, A::drain_wbuf, 0};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, 0};
            case S::closing_eof_R: return {S::closing_eof_RC, A::defer_park, 0};
            case S::closing_eof_RW: return {S::closing_eof_RCW, A::defer_park, 0};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, 0};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, 0};
            default: break;
            }
            break;
        case guard::idle_clear:
            switch (s) {
            case S::draining_act: return {S::draining_act, A::drain_wbuf, 0};
            case S::draining_act_W: return {S::draining_act_W, A::drain_wbuf, 0};
            case S::draining_act_C: return {S::draining_act_C, A::drain_wbuf, 0};
            case S::draining_act_CW: return {S::draining_act_CW, A::drain_wbuf, 0};
            case S::draining_act_R: return {S::draining_act_R, A::drain_wbuf, 0};
            case S::draining_act_RW: return {S::draining_act_RW, A::drain_wbuf, 0};
            case S::draining_act_RC: return {S::draining_act_RC, A::drain_wbuf, 0};
            case S::draining_act_RCW: return {S::draining_act_RCW, A::drain_wbuf, 0};
            case S::draining_pau: return {S::draining_pau, A::drain_wbuf, 0};
            case S::draining_pau_W: return {S::draining_pau_W, A::drain_wbuf, 0};
            case S::draining_pau_C: return {S::draining_pau_C, A::drain_wbuf, 0};
            case S::draining_pau_CW: return {S::draining_pau_CW, A::drain_wbuf, 0};
            case S::draining_eof: return {S::draining_eof, A::drain_wbuf, 0};
            case S::draining_eof_W: return {S::draining_eof_W, A::drain_wbuf, 0};
            case S::draining_eof_C: return {S::draining_eof_C, A::drain_wbuf, 0};
            case S::draining_eof_CW: return {S::draining_eof_CW, A::drain_wbuf, 0};
            case S::draining_eof_R: return {S::draining_eof_R, A::drain_wbuf, 0};
            case S::draining_eof_RW: return {S::draining_eof_RW, A::drain_wbuf, 0};
            case S::draining_eof_RC: return {S::draining_eof_RC, A::drain_wbuf, 0};
            case S::draining_eof_RCW: return {S::draining_eof_RCW, A::drain_wbuf, 0};
            case S::closing_act: return {S::closing_act, A::drain_wbuf, 0};
            case S::closing_act_W: return {S::closing_act_W, A::drain_wbuf, 0};
            case S::closing_act_C: return {S::closing_act_C, A::drain_wbuf, 0};
            case S::closing_act_CW: return {S::closing_act_CW, A::drain_wbuf, 0};
            case S::closing_act_R: return {S::closing_act_R, A::drain_wbuf, 0};
            case S::closing_act_RW: return {S::closing_act_RW, A::drain_wbuf, 0};
            case S::closing_act_RC: return {S::closing_act_RC, A::drain_wbuf, 0};
            case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, 0};
            case S::closing_eof: return {S::closing_eof, A::drain_wbuf, 0};
            case S::closing_eof_W: return {S::closing_eof_W, A::drain_wbuf, 0};
            case S::closing_eof_C: return {S::closing_eof_C, A::drain_wbuf, 0};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, 0};
            case S::closing_eof_R: return {S::closing_eof_R, A::drain_wbuf, 0};
            case S::closing_eof_RW: return {S::closing_eof_RW, A::drain_wbuf, 0};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, 0};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, 0};
            default: break;
            }
            break;
        default: break;
        }
        break;
    case conn_event::input_defer_failed:
        switch (s) {
        case S::draining_act: return {S::draining_act_C, A::defer_park, 0};
        case S::draining_act_W: return {S::draining_act_CW, A::defer_park, 0};
        case S::draining_act_C: return {S::draining_act_C, A::drain_wbuf, 0};
        case S::draining_act_CW: return {S::draining_act_CW, A::drain_wbuf, 0};
        case S::draining_act_R: return {S::draining_act_RC, A::defer_park, 0};
        case S::draining_act_RW: return {S::draining_act_RCW, A::defer_park, 0};
        case S::draining_act_RC: return {S::draining_act_RC, A::drain_wbuf, 0};
        case S::draining_act_RCW: return {S::draining_act_RCW, A::drain_wbuf, 0};
        case S::draining_pau: return {S::draining_pau_C, A::defer_park, 0};
        case S::draining_pau_W: return {S::draining_pau_CW, A::defer_park, 0};
        case S::draining_pau_C: return {S::draining_pau_C, A::drain_wbuf, 0};
        case S::draining_pau_CW: return {S::draining_pau_CW, A::drain_wbuf, 0};
        case S::draining_eof: return {S::draining_eof_C, A::defer_park, 0};
        case S::draining_eof_W: return {S::draining_eof_CW, A::defer_park, 0};
        case S::draining_eof_C: return {S::draining_eof_C, A::drain_wbuf, 0};
        case S::draining_eof_CW: return {S::draining_eof_CW, A::drain_wbuf, 0};
        case S::draining_eof_R: return {S::draining_eof_RC, A::defer_park, 0};
        case S::draining_eof_RW: return {S::draining_eof_RCW, A::defer_park, 0};
        case S::draining_eof_RC: return {S::draining_eof_RC, A::drain_wbuf, 0};
        case S::draining_eof_RCW: return {S::draining_eof_RCW, A::drain_wbuf, 0};
        case S::closing_act: return {S::closing_act_C, A::defer_park, 0};
        case S::closing_act_W: return {S::closing_act_CW, A::defer_park, 0};
        case S::closing_act_C: return {S::closing_act_C, A::drain_wbuf, 0};
        case S::closing_act_CW: return {S::closing_act_CW, A::drain_wbuf, 0};
        case S::closing_act_R: return {S::closing_act_RC, A::defer_park, 0};
        case S::closing_act_RW: return {S::closing_act_RCW, A::defer_park, 0};
        case S::closing_act_RC: return {S::closing_act_RC, A::drain_wbuf, 0};
        case S::closing_act_RCW: return {S::closing_act_RCW, A::drain_wbuf, 0};
        case S::closing_eof: return {S::closing_eof_C, A::defer_park, 0};
        case S::closing_eof_W: return {S::closing_eof_CW, A::defer_park, 0};
        case S::closing_eof_C: return {S::closing_eof_C, A::drain_wbuf, 0};
        case S::closing_eof_CW: return {S::closing_eof_CW, A::drain_wbuf, 0};
        case S::closing_eof_R: return {S::closing_eof_RC, A::defer_park, 0};
        case S::closing_eof_RW: return {S::closing_eof_RCW, A::defer_park, 0};
        case S::closing_eof_RC: return {S::closing_eof_RC, A::drain_wbuf, 0};
        case S::closing_eof_RCW: return {S::closing_eof_RCW, A::drain_wbuf, 0};
        case S::fin_sent_act: return {S::fin_sent_act_C, A::defer_park, 0};
        case S::fin_sent_act_W: return {S::fin_sent_act_CW, A::defer_park, 0};
        case S::fin_sent_act_C: return {S::fin_sent_act_C, A::drain_wbuf, 0};
        case S::fin_sent_act_CW: return {S::fin_sent_act_CW, A::drain_wbuf, 0};
        case S::fin_sent_act_R: return {S::fin_sent_act_RC, A::defer_park, 0};
        case S::fin_sent_act_RW: return {S::fin_sent_act_RCW, A::defer_park, 0};
        case S::fin_sent_act_RC: return {S::fin_sent_act_RC, A::drain_wbuf, 0};
        case S::fin_sent_act_RCW: return {S::fin_sent_act_RCW, A::drain_wbuf, 0};
        case S::fin_sent_eof: return {S::fin_sent_eof_C, A::defer_park, 0};
        case S::fin_sent_eof_W: return {S::fin_sent_eof_CW, A::defer_park, 0};
        case S::fin_sent_eof_C: return {S::fin_sent_eof_C, A::drain_wbuf, 0};
        case S::fin_sent_eof_CW: return {S::fin_sent_eof_CW, A::drain_wbuf, 0};
        case S::fin_sent_eof_R: return {S::fin_sent_eof_RC, A::defer_park, 0};
        case S::fin_sent_eof_RW: return {S::fin_sent_eof_RCW, A::defer_park, 0};
        case S::fin_sent_eof_RC: return {S::fin_sent_eof_RC, A::drain_wbuf, 0};
        case S::fin_sent_eof_RCW: return {S::fin_sent_eof_RCW, A::drain_wbuf, 0};
        case S::closed_act: return {S::closed_act_C, A::defer_park, 0};
        case S::closed_act_C: return {S::closed_act_C, A::drain_wbuf, 0};
        case S::closed_eof: return {S::closed_eof_C, A::defer_park, 0};
        case S::closed_eof_C: return {S::closed_eof_C, A::drain_wbuf, 0};
        default: break;
        }
        break;
    case conn_event::peer_fin:
        switch (g) {
        case guard::wbuf_busy:
            switch (s) {
            default: break;
            }
            break;
        case guard::owed:
            switch (s) {
            case S::open_eof: return {S::open_eof_P, A::defer_peer_closed, 0};
            case S::open_eof_W: return {S::open_eof_PW, A::defer_peer_closed, 0};
            case S::open_eof_P: return {S::open_eof_P, A::close_graceful, 0};
            case S::open_eof_PW: return {S::open_eof_PW, A::close_graceful, 0};
            case S::draining_eof: return {S::draining_eof_P, A::defer_peer_closed, 0};
            case S::draining_eof_W: return {S::draining_eof_PW, A::defer_peer_closed, 0};
            case S::draining_eof_C: return {S::draining_eof_P, A::defer_peer_closed, 0};
            case S::draining_eof_CW: return {S::draining_eof_PW, A::defer_peer_closed, 0};
            case S::draining_eof_P: return {S::draining_eof_P, A::close_graceful, 0};
            case S::draining_eof_PW: return {S::draining_eof_PW, A::close_graceful, 0};
            case S::draining_eof_R: return {S::draining_eof_RP, A::defer_peer_closed, 0};
            case S::draining_eof_RW: return {S::draining_eof_RPW, A::defer_peer_closed, 0};
            case S::draining_eof_RC: return {S::draining_eof_RP, A::defer_peer_closed, 0};
            case S::draining_eof_RCW: return {S::draining_eof_RPW, A::defer_peer_closed, 0};
            case S::draining_eof_RP: return {S::draining_eof_RP, A::close_graceful, 0};
            case S::draining_eof_RPW: return {S::draining_eof_RPW, A::close_graceful, 0};
            case S::closing_eof: return {S::closing_eof, A::send_fin, 0};
            case S::closing_eof_W: return {S::closing_eof_W, A::send_fin, 0};
            case S::closing_eof_C: return {S::closing_eof_C, A::send_fin, 0};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::send_fin, 0};
            case S::closing_eof_P: return {S::closing_eof_P, A::send_fin, 0};
            case S::closing_eof_PW: return {S::closing_eof_PW, A::send_fin, 0};
            case S::closing_eof_R: return {S::closing_eof_R, A::send_fin, 0};
            case S::closing_eof_RW: return {S::closing_eof_RW, A::send_fin, 0};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::send_fin, 0};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::send_fin, 0};
            case S::closing_eof_RP: return {S::closing_eof_RP, A::send_fin, 0};
            case S::closing_eof_RPW: return {S::closing_eof_RPW, A::send_fin, 0};
            case S::fin_sent_eof: return {S::fin_sent_eof, A::close_graceful, 0};
            case S::fin_sent_eof_W: return {S::fin_sent_eof_W, A::close_graceful, 0};
            case S::fin_sent_eof_C: return {S::fin_sent_eof_C, A::close_graceful, 0};
            case S::fin_sent_eof_CW: return {S::fin_sent_eof_CW, A::close_graceful, 0};
            case S::fin_sent_eof_P: return {S::fin_sent_eof_P, A::close_graceful, 0};
            case S::fin_sent_eof_PW: return {S::fin_sent_eof_PW, A::close_graceful, 0};
            case S::fin_sent_eof_R: return {S::fin_sent_eof_R, A::close_graceful, 0};
            case S::fin_sent_eof_RW: return {S::fin_sent_eof_RW, A::close_graceful, 0};
            case S::fin_sent_eof_RC: return {S::fin_sent_eof_RC, A::close_graceful, 0};
            case S::fin_sent_eof_RCW: return {S::fin_sent_eof_RCW, A::close_graceful, 0};
            case S::fin_sent_eof_RP: return {S::fin_sent_eof_RP, A::close_graceful, 0};
            case S::fin_sent_eof_RPW: return {S::fin_sent_eof_RPW, A::close_graceful, 0};
            default: break;
            }
            break;
        case guard::clear:
            switch (s) {
            case S::open_eof: return {S::open_eof, A::close_graceful, 0};
            case S::open_eof_W: return {S::open_eof_W, A::close_graceful, 0};
            case S::open_eof_P: return {S::open_eof_P, A::close_graceful, 0};
            case S::open_eof_PW: return {S::open_eof_PW, A::close_graceful, 0};
            case S::draining_eof: return {S::draining_eof, A::close_graceful, 0};
            case S::draining_eof_W: return {S::draining_eof_W, A::close_graceful, 0};
            case S::draining_eof_C: return {S::draining_eof_C, A::close_graceful, 0};
            case S::draining_eof_CW: return {S::draining_eof_CW, A::close_graceful, 0};
            case S::draining_eof_P: return {S::draining_eof_P, A::close_graceful, 0};
            case S::draining_eof_PW: return {S::draining_eof_PW, A::close_graceful, 0};
            case S::draining_eof_R: return {S::draining_eof_R, A::close_graceful, 0};
            case S::draining_eof_RW: return {S::draining_eof_RW, A::close_graceful, 0};
            case S::draining_eof_RC: return {S::draining_eof_RC, A::close_graceful, 0};
            case S::draining_eof_RCW: return {S::draining_eof_RCW, A::close_graceful, 0};
            case S::draining_eof_RP: return {S::draining_eof_RP, A::close_graceful, 0};
            case S::draining_eof_RPW: return {S::draining_eof_RPW, A::close_graceful, 0};
            case S::closing_eof: return {S::closing_eof, A::send_fin, 0};
            case S::closing_eof_W: return {S::closing_eof_W, A::send_fin, 0};
            case S::closing_eof_C: return {S::closing_eof_C, A::send_fin, 0};
            case S::closing_eof_CW: return {S::closing_eof_CW, A::send_fin, 0};
            case S::closing_eof_P: return {S::closing_eof_P, A::send_fin, 0};
            case S::closing_eof_PW: return {S::closing_eof_PW, A::send_fin, 0};
            case S::closing_eof_R: return {S::closing_eof_R, A::send_fin, 0};
            case S::closing_eof_RW: return {S::closing_eof_RW, A::send_fin, 0};
            case S::closing_eof_RC: return {S::closing_eof_RC, A::send_fin, 0};
            case S::closing_eof_RCW: return {S::closing_eof_RCW, A::send_fin, 0};
            case S::closing_eof_RP: return {S::closing_eof_RP, A::send_fin, 0};
            case S::closing_eof_RPW: return {S::closing_eof_RPW, A::send_fin, 0};
            case S::fin_sent_eof: return {S::fin_sent_eof, A::close_graceful, 0};
            case S::fin_sent_eof_W: return {S::fin_sent_eof_W, A::close_graceful, 0};
            case S::fin_sent_eof_C: return {S::fin_sent_eof_C, A::close_graceful, 0};
            case S::fin_sent_eof_CW: return {S::fin_sent_eof_CW, A::close_graceful, 0};
            case S::fin_sent_eof_P: return {S::fin_sent_eof_P, A::close_graceful, 0};
            case S::fin_sent_eof_PW: return {S::fin_sent_eof_PW, A::close_graceful, 0};
            case S::fin_sent_eof_R: return {S::fin_sent_eof_R, A::close_graceful, 0};
            case S::fin_sent_eof_RW: return {S::fin_sent_eof_RW, A::close_graceful, 0};
            case S::fin_sent_eof_RC: return {S::fin_sent_eof_RC, A::close_graceful, 0};
            case S::fin_sent_eof_RCW: return {S::fin_sent_eof_RCW, A::close_graceful, 0};
            case S::fin_sent_eof_RP: return {S::fin_sent_eof_RP, A::close_graceful, 0};
            case S::fin_sent_eof_RPW: return {S::fin_sent_eof_RPW, A::close_graceful, 0};
            default: break;
            }
            break;
        default: break;
        }
        break;
    case conn_event::fin_sent:
        switch (s) {
        case S::closing_act: return {S::fin_sent_act, A::none, 0};
        case S::closing_act_W: return {S::fin_sent_act_W, A::none, 0};
        case S::closing_act_C: return {S::fin_sent_act_C, A::none, 0};
        case S::closing_act_CW: return {S::fin_sent_act_CW, A::none, 0};
        case S::closing_act_R: return {S::fin_sent_act_R, A::none, 0};
        case S::closing_act_RW: return {S::fin_sent_act_RW, A::none, 0};
        case S::closing_act_RC: return {S::fin_sent_act_RC, A::none, 0};
        case S::closing_act_RCW: return {S::fin_sent_act_RCW, A::none, 0};
        case S::closing_eof: return {S::fin_sent_eof, A::none, 0};
        case S::closing_eof_W: return {S::fin_sent_eof_W, A::none, 0};
        case S::closing_eof_C: return {S::fin_sent_eof_C, A::none, 0};
        case S::closing_eof_CW: return {S::fin_sent_eof_CW, A::none, 0};
        case S::closing_eof_P: return {S::fin_sent_eof_P, A::none, 0};
        case S::closing_eof_PW: return {S::fin_sent_eof_PW, A::none, 0};
        case S::closing_eof_R: return {S::fin_sent_eof_R, A::none, 0};
        case S::closing_eof_RW: return {S::fin_sent_eof_RW, A::none, 0};
        case S::closing_eof_RC: return {S::fin_sent_eof_RC, A::none, 0};
        case S::closing_eof_RCW: return {S::fin_sent_eof_RCW, A::none, 0};
        case S::closing_eof_RP: return {S::fin_sent_eof_RP, A::none, 0};
        case S::closing_eof_RPW: return {S::fin_sent_eof_RPW, A::none, 0};
        default: break;
        }
        break;
    case conn_event::closed:
        switch (s) {
        case S::open_act: return {S::closed_act, A::none, 0};
        case S::open_act_W: return {S::closed_act, A::none, 0};
        case S::open_pau: return {S::closed_act, A::none, 0};
        case S::open_pau_W: return {S::closed_act, A::none, 0};
        case S::open_eof: return {S::closed_eof, A::none, 0};
        case S::open_eof_W: return {S::closed_eof, A::none, 0};
        case S::open_eof_P: return {S::closed_eof_P, A::none, 0};
        case S::open_eof_PW: return {S::closed_eof_P, A::none, 0};
        case S::draining_act: return {S::closed_act, A::none, 0};
        case S::draining_act_W: return {S::closed_act, A::none, 0};
        case S::draining_act_C: return {S::closed_act_C, A::none, 0};
        case S::draining_act_CW: return {S::closed_act_C, A::none, 0};
        case S::draining_act_R: return {S::closed_act, A::none, 0};
        case S::draining_act_RW: return {S::closed_act, A::none, 0};
        case S::draining_act_RC: return {S::closed_act_C, A::none, 0};
        case S::draining_act_RCW: return {S::closed_act_C, A::none, 0};
        case S::draining_pau: return {S::closed_act, A::none, 0};
        case S::draining_pau_W: return {S::closed_act, A::none, 0};
        case S::draining_pau_C: return {S::closed_act_C, A::none, 0};
        case S::draining_pau_CW: return {S::closed_act_C, A::none, 0};
        case S::draining_eof: return {S::closed_eof, A::none, 0};
        case S::draining_eof_W: return {S::closed_eof, A::none, 0};
        case S::draining_eof_C: return {S::closed_eof_C, A::none, 0};
        case S::draining_eof_CW: return {S::closed_eof_C, A::none, 0};
        case S::draining_eof_P: return {S::closed_eof_P, A::none, 0};
        case S::draining_eof_PW: return {S::closed_eof_P, A::none, 0};
        case S::draining_eof_R: return {S::closed_eof, A::none, 0};
        case S::draining_eof_RW: return {S::closed_eof, A::none, 0};
        case S::draining_eof_RC: return {S::closed_eof_C, A::none, 0};
        case S::draining_eof_RCW: return {S::closed_eof_C, A::none, 0};
        case S::draining_eof_RP: return {S::closed_eof_P, A::none, 0};
        case S::draining_eof_RPW: return {S::closed_eof_P, A::none, 0};
        case S::closing_act: return {S::closed_act, A::none, 0};
        case S::closing_act_W: return {S::closed_act, A::none, 0};
        case S::closing_act_C: return {S::closed_act_C, A::none, 0};
        case S::closing_act_CW: return {S::closed_act_C, A::none, 0};
        case S::closing_act_R: return {S::closed_act, A::none, 0};
        case S::closing_act_RW: return {S::closed_act, A::none, 0};
        case S::closing_act_RC: return {S::closed_act_C, A::none, 0};
        case S::closing_act_RCW: return {S::closed_act_C, A::none, 0};
        case S::closing_eof: return {S::closed_eof, A::none, 0};
        case S::closing_eof_W: return {S::closed_eof, A::none, 0};
        case S::closing_eof_C: return {S::closed_eof_C, A::none, 0};
        case S::closing_eof_CW: return {S::closed_eof_C, A::none, 0};
        case S::closing_eof_P: return {S::closed_eof_P, A::none, 0};
        case S::closing_eof_PW: return {S::closed_eof_P, A::none, 0};
        case S::closing_eof_R: return {S::closed_eof, A::none, 0};
        case S::closing_eof_RW: return {S::closed_eof, A::none, 0};
        case S::closing_eof_RC: return {S::closed_eof_C, A::none, 0};
        case S::closing_eof_RCW: return {S::closed_eof_C, A::none, 0};
        case S::closing_eof_RP: return {S::closed_eof_P, A::none, 0};
        case S::closing_eof_RPW: return {S::closed_eof_P, A::none, 0};
        case S::fin_sent_act: return {S::closed_act, A::none, 0};
        case S::fin_sent_act_W: return {S::closed_act, A::none, 0};
        case S::fin_sent_act_C: return {S::closed_act_C, A::none, 0};
        case S::fin_sent_act_CW: return {S::closed_act_C, A::none, 0};
        case S::fin_sent_act_R: return {S::closed_act, A::none, 0};
        case S::fin_sent_act_RW: return {S::closed_act, A::none, 0};
        case S::fin_sent_act_RC: return {S::closed_act_C, A::none, 0};
        case S::fin_sent_act_RCW: return {S::closed_act_C, A::none, 0};
        case S::fin_sent_eof: return {S::closed_eof, A::none, 0};
        case S::fin_sent_eof_W: return {S::closed_eof, A::none, 0};
        case S::fin_sent_eof_C: return {S::closed_eof_C, A::none, 0};
        case S::fin_sent_eof_CW: return {S::closed_eof_C, A::none, 0};
        case S::fin_sent_eof_P: return {S::closed_eof_P, A::none, 0};
        case S::fin_sent_eof_PW: return {S::closed_eof_P, A::none, 0};
        case S::fin_sent_eof_R: return {S::closed_eof, A::none, 0};
        case S::fin_sent_eof_RW: return {S::closed_eof, A::none, 0};
        case S::fin_sent_eof_RC: return {S::closed_eof_C, A::none, 0};
        case S::fin_sent_eof_RCW: return {S::closed_eof_C, A::none, 0};
        case S::fin_sent_eof_RP: return {S::closed_eof_P, A::none, 0};
        case S::fin_sent_eof_RPW: return {S::closed_eof_P, A::none, 0};
        default: break;
        }
        break;
    }
    return {s, A::none, 0};
}
// ===== END GENERATED =====
