// verify/close_state_machine.cpp
//
// The close protocol as a table. decide() is a pure function of
// (phase, event, facts), so every combination of them can be enumerated here in
// milliseconds -- no socket, no thread, no epoll set, no 33-configuration
// matrix. That is the whole point of R3: the state machine used to be five
// functions that could only be checked by running the server.
//
// Build and run (Linux, alongside the other suites):
//     g++ -std=c++17 -Wall -Wextra -Iinclude verify/close_state_machine.cpp
//
// Build and run on a host without epoll (extracts just this section):
//     python verify/evidence/extract_close_sm.py --cxx g++
//
// The oracle below is transcribed from docs/architecture/r3-baseline.md
// section 5, independently of decide()'s own code. Two transcriptions of one
// table can disagree; one transcription checked against itself cannot.

#ifdef CLOSE_SM_STANDALONE
// The extractor supplies the CloseState section verbatim; the header itself
// cannot be included off Linux.
#else
#include "tcp-server-mt.h"
#endif

#include <cstdio>
#include <cstring>

namespace {

int g_pass = 0;
int g_fail = 0;

const char* phaseName(industrial::ClosePhase p) {
    switch (p) {
    case industrial::ClosePhase::Open:    return "Open";
    case industrial::ClosePhase::Closing: return "Closing";
    case industrial::ClosePhase::FinSent: return "FinSent";
    case industrial::ClosePhase::Torn:    return "Torn";
    }
    return "?";
}

const char* eventName(industrial::CloseEvent e) {
    switch (e) {
    case industrial::CloseEvent::AppClose:      return "AppClose";
    case industrial::CloseEvent::PeerFin:       return "PeerFin";
    case industrial::CloseEvent::DrainDone:     return "DrainDone";
    case industrial::CloseEvent::BudgetSpent:   return "BudgetSpent";
    case industrial::CloseEvent::LingerExpired: return "LingerExpired";
    case industrial::CloseEvent::IoError:       return "IoError";
    }
    return "?";
}

const char* actionName(industrial::CloseAction a) {
    switch (a) {
    case industrial::CloseAction::None:             return "None";
    case industrial::CloseAction::Park:             return "Park";
    case industrial::CloseAction::SendFin:          return "SendFin";
    case industrial::CloseAction::RedrainInput:     return "RedrainInput";
    case industrial::CloseAction::DeferPark:        return "DeferPark";
    case industrial::CloseAction::TeardownGraceful: return "TeardownGraceful";
    case industrial::CloseAction::TeardownAbort:    return "TeardownAbort";
    }
    return "?";
}

using industrial::CloseAction;
using industrial::CloseEvent;
using industrial::CloseFacts;
using industrial::ClosePhase;
using industrial::CloseRound;

// ---- the oracle: baseline section 5, written out again on purpose -----------
CloseAction expected(ClosePhase p, CloseEvent e, const CloseFacts& f) {
    // Torn is terminal. Every function that can reach a torn connection
    // returns at its head, so no event has any effect on it.
    if (p == ClosePhase::Torn) return CloseAction::None;

    // drainInputThenClose() returns at its head when the FIN has already gone.
    if (e == CloseEvent::BudgetSpent && p == ClosePhase::FinSent)
        return CloseAction::None;

    switch (e) {
    case CloseEvent::IoError:
        return CloseAction::TeardownAbort;

    case CloseEvent::LingerExpired:
        // doTick's sweep selects on `closing`, which a FIN does not clear.
        if (p == ClosePhase::Closing || p == ClosePhase::FinSent)
            return CloseAction::TeardownAbort;
        return CloseAction::None;

    case CloseEvent::AppClose:
        if (p == ClosePhase::FinSent) return CloseAction::None;
        return f.wbufEmpty ? CloseAction::SendFin : CloseAction::Park;

    case CloseEvent::PeerFin:
        if (!f.wbufEmpty) return CloseAction::Park;
        if (p == ClosePhase::Closing) return CloseAction::SendFin;
        if (p == ClosePhase::FinSent) return CloseAction::TeardownGraceful;
        if (f.round == CloseRound::None && f.pending)
            return CloseAction::DeferPark;
        return CloseAction::TeardownGraceful;

    case CloseEvent::DrainDone:
        // Stricter than baseline section 5 on purpose: the table there says
        // "Closing -> SendFin", but a FIN over a non-empty wbuf is the one
        // mistake the protocol exists to prevent, so the wbuf test is part of
        // the contract rather than an assumption about the caller.
        if (p != ClosePhase::Closing || !f.wbufEmpty) return CloseAction::None;
        return CloseAction::SendFin;

    case CloseEvent::BudgetSpent:
        if (f.served && f.budgetLeft && !f.rbufEmpty)
            return CloseAction::RedrainInput;
        if (!f.served && f.round == CloseRound::None && f.pending)
            return CloseAction::DeferPark;
        if (f.round == CloseRound::PeerClosed) return CloseAction::None;
        if (!f.served || f.round == CloseRound::Close) return CloseAction::Park;
        return CloseAction::DeferPark;
    }
    return CloseAction::None;
}

void check(const char* what, bool ok, const char* detail) {
    if (ok) {
        ++g_pass;
    } else {
        ++g_fail;
        std::printf("  FAIL  %s  %s\n", what, detail ? detail : "");
    }
}

// Canonical facts for the readable matrix: the combination each event is
// usually delivered with.
CloseFacts canonical(CloseEvent e) {
    CloseFacts f;
    switch (e) {
    case CloseEvent::AppClose:
        f.wbufEmpty = true;
        break;
    case CloseEvent::PeerFin:
        f.wbufEmpty = true;
        f.pending = false;
        f.round = CloseRound::None;
        break;
    case CloseEvent::DrainDone:
        f.wbufEmpty = true;
        break;
    case CloseEvent::BudgetSpent:
        f.served = false;
        f.pending = false;
        f.round = CloseRound::None;
        break;
    case CloseEvent::LingerExpired:
    case CloseEvent::IoError:
        break;
    }
    return f;
}

}  // namespace

int main() {
    const ClosePhase phases[] = {ClosePhase::Open, ClosePhase::Closing,
                                 ClosePhase::FinSent, ClosePhase::Torn};
    const CloseEvent events[] = {CloseEvent::AppClose, CloseEvent::PeerFin,
                                 CloseEvent::DrainDone,
                                 CloseEvent::BudgetSpent,
                                 CloseEvent::LingerExpired,
                                 CloseEvent::IoError};

    // ---- 1. the readable matrix: 4 phases x 6 events, canonical facts -------
    std::printf("\nphase x event (canonical facts)\n");
    std::printf("%-10s", "");
    for (auto e : events) std::printf("%-18s", eventName(e));
    std::printf("\n");
    for (auto p : phases) {
        std::printf("%-10s", phaseName(p));
        for (auto e : events) {
            const CloseAction a = industrial::decide(p, e, canonical(e));
            std::printf("%-18s", actionName(a));
        }
        std::printf("\n");
    }
    std::printf("\n");

    // ---- 2. exhaustive: every phase x event x every fact combination ---------
    // 4 x 6 x (2^4 booleans x 3 rounds) = 1152 cells. Small enough to be
    // exhaustive rather than representative, which is the only way a table test
    // is worth writing.
    int cells = 0;
    for (auto p : phases) {
        for (auto e : events) {
            for (int bits = 0; bits < 16; ++bits) {
                for (int r = 0; r < 3; ++r) {
                    CloseFacts f;
                    f.wbufEmpty  = (bits & 1) != 0;
                    f.rbufEmpty  = (bits & 2) != 0;
                    f.pending    = (bits & 4) != 0;
                    f.budgetLeft = (bits & 8) != 0;
                    f.round      = static_cast<CloseRound>(r);
                    // `served` is only meaningful for BudgetSpent; it is varied
                    // there and pinned elsewhere.
                    for (int s = 0; s < 2; ++s) {
                        f.served = s != 0;
                        ++cells;
                        const CloseAction got = industrial::decide(p, e, f);
                        const CloseAction want = expected(p, e, f);
                        if (got != want) {
                            char buf[160];
                            std::snprintf(buf, sizeof(buf),
                                          "%s/%s bits=%d round=%d served=%d "
                                          "got=%s want=%s",
                                          phaseName(p), eventName(e), bits, r,
                                          s, actionName(got),
                                          actionName(want));
                            check("exhaustive", false, buf);
                        } else {
                            check("exhaustive", true, nullptr);
                        }
                    }
                }
            }
        }
    }
    std::printf("exhaustive: %d cells\n", cells);

    // ---- 3. the 26 rows of baseline section 5, where they are expressible ----
    CloseFacts f;

    // parkClose is idempotent once the FIN has gone, and a no-op when torn.
    f = canonical(CloseEvent::AppClose);
    check("parkClose after FIN is a no-op",
          industrial::decide(ClosePhase::FinSent, CloseEvent::AppClose, f) ==
              CloseAction::None, nullptr);
    check("parkClose on a torn conn is a no-op",
          industrial::decide(ClosePhase::Torn, CloseEvent::AppClose, f) ==
              CloseAction::None, nullptr);

    // A FIN must never be sent while wbuf still holds bytes: shutdown() would
    // discard them and the peer would see a clean end to a truncated stream.
    f.wbufEmpty = false;
    check("no FIN over a non-empty wbuf (AppClose)",
          industrial::decide(ClosePhase::Closing, CloseEvent::AppClose, f) ==
              CloseAction::Park, nullptr);
    check("no FIN over a non-empty wbuf (DrainDone)",
          industrial::decide(ClosePhase::Closing, CloseEvent::DrainDone, f) !=
              CloseAction::SendFin, nullptr);
    check("no FIN over a non-empty wbuf (PeerFin)",
          industrial::decide(ClosePhase::Closing, CloseEvent::PeerFin, f) ==
              CloseAction::Park, nullptr);

    // PeerFin on a connection that is not closing and owes nothing: tear down
    // gracefully, never abort (P0-5 was an RST the peer never earned).
    f = canonical(CloseEvent::PeerFin);
    check("PeerFin on an open, owed-nothing conn is graceful",
          industrial::decide(ClosePhase::Open, CloseEvent::PeerFin, f) ==
              CloseAction::TeardownGraceful, nullptr);

    // PeerFin with replies still reserved gets exactly one queue round.
    f.pending = true;
    check("PeerFin with reserved bytes defers one round",
          industrial::decide(ClosePhase::Open, CloseEvent::PeerFin, f) ==
              CloseAction::DeferPark, nullptr);
    f.round = CloseRound::PeerClosed;
    check("PeerFin does not defer twice",
          industrial::decide(ClosePhase::Open, CloseEvent::PeerFin, f) ==
              CloseAction::TeardownGraceful, nullptr);

    // A parked close always ends in a FIN once the drain is done.
    f = canonical(CloseEvent::DrainDone);
    check("DrainDone while closing sends the FIN",
          industrial::decide(ClosePhase::Closing, CloseEvent::DrainDone, f) ==
              CloseAction::SendFin, nullptr);
    check("DrainDone while open does nothing",
          industrial::decide(ClosePhase::Open, CloseEvent::DrainDone, f) ==
              CloseAction::None, nullptr);

    // The linger deadline aborts, because a deadline is all it can be sure of.
    f = CloseFacts();
    check("linger aborts a closing conn",
          industrial::decide(ClosePhase::Closing, CloseEvent::LingerExpired,
                             f) == CloseAction::TeardownAbort, nullptr);
    check("linger aborts a fin-sent conn",
          industrial::decide(ClosePhase::FinSent, CloseEvent::LingerExpired,
                             f) == CloseAction::TeardownAbort, nullptr);
    check("linger ignores an open conn",
          industrial::decide(ClosePhase::Open, CloseEvent::LingerExpired, f) ==
              CloseAction::None, nullptr);

    // IoError aborts everything that is not already gone.
    check("IoError aborts while open",
          industrial::decide(ClosePhase::Open, CloseEvent::IoError, f) ==
              CloseAction::TeardownAbort, nullptr);
    check("IoError aborts after the FIN",
          industrial::decide(ClosePhase::FinSent, CloseEvent::IoError, f) ==
              CloseAction::TeardownAbort, nullptr);
    check("IoError ignores a torn conn",
          industrial::decide(ClosePhase::Torn, CloseEvent::IoError, f) ==
              CloseAction::None, nullptr);

    // The close-input budget: another round needs progress, budget and buffered
    // input all at once. Any one missing parks instead.
    f = canonical(CloseEvent::BudgetSpent);
    f.served = true; f.budgetLeft = true; f.rbufEmpty = false;
    check("budget left with input buffered re-drains",
          industrial::decide(ClosePhase::Closing, CloseEvent::BudgetSpent, f) ==
              CloseAction::RedrainInput, nullptr);
    f.rbufEmpty = true;
    check("budget left with nothing buffered does not re-drain",
          industrial::decide(ClosePhase::Closing, CloseEvent::BudgetSpent, f) !=
              CloseAction::RedrainInput, nullptr);
    f.rbufEmpty = false; f.budgetLeft = false;
    check("exhausted budget does not re-drain",
          industrial::decide(ClosePhase::Closing, CloseEvent::BudgetSpent, f) !=
              CloseAction::RedrainInput, nullptr);

    // finishPeerClosed owns the round: a close that deferred for PeerFin must
    // not park over the replies that deferral exists to protect.
    f = canonical(CloseEvent::BudgetSpent);
    f.round = CloseRound::PeerClosed;
    check("a PeerClosed round is never parked over",
          industrial::decide(ClosePhase::Closing, CloseEvent::BudgetSpent, f) ==
              CloseAction::None, nullptr);

    // Nothing that has already sent its FIN gets another FIN.
    for (auto e : events) {
        CloseFacts g = canonical(e);
        check("no second FIN",
              industrial::decide(ClosePhase::FinSent, e, g) !=
                  CloseAction::SendFin, eventName(e));
    }

    std::printf("\n%s  pass=%d fail=%d\n",
                g_fail == 0 ? "PASS" : "FAIL", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
