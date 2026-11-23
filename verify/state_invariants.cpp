// verify/state_invariants.cpp -- one place for the combinations that must
// never be observed.
//
// The connection state is not one state machine. It is four independent axes:
//   interest  -- write_armed, read_paused, read_pending
//   transport -- fd, peer_closed
//   close     -- fsm.closing, fsm.fin_sent, fsm.deferred_round
//   activity  -- fsm.draining_input
// A single enum would have to name every product of those axes, most of which
// are legal, and the table would stop being readable. What is testable is the
// much smaller set of combinations that have caused bugs here: a paused read
// left on the continuation list, a peer FIN treated as "nothing owed", a FIN
// over buffered bytes, and interest or continuations left on a detached fd.
//
// Three layers, each checking the next one rather than restating it:
//   1. the invariant table -- legal and illegal axis combinations go through
//      connection::state_violation(), including the reasons;
//   2. the decision table -- real close_fsm methods are called from defined
//      starting states, and the resulting state must be legal;
//   3. the guard itself -- a child process with an illegal combo must die by
//      SIGABRT in a debug build, so the assertions cannot silently vanish.
//
// Linux only, like everything under verify/.

#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "harness.h"

namespace {

using industrial::close_action;
using industrial::connection;
using industrial::drain_outcome;
using deferred_kind = industrial::close_fsm::deferred_kind;

// A compact description of the axes. The default is a healthy live
// connection: no interest armed, no close requested, no FIN seen.
struct axes {
    int  fd = 7;
    bool is_closed = false;
    bool closing = false;
    bool fin_sent = false;
    bool read_paused = false;
    bool peer_closed = false;
    bool write_armed = false;
    bool read_pending = false;
    bool draining = false;
    deferred_kind deferred = deferred_kind::none;
    size_t rbuf_bytes = 0;
    size_t wbuf_bytes = 0;
};

void apply(connection& c, const axes& a) {
    c.id = 1;
    c.fd = a.fd;
    c.rbuf.init(64);
    c.wbuf.init(64);
    c.fsm.attach(&c.wbuf, &c.rbuf, &c.pending_bytes);
    c.fsm.is_closed.store(a.is_closed, std::memory_order_relaxed);
    c.fsm.closing.store(a.closing, std::memory_order_relaxed);
    c.fsm.fin_sent = a.fin_sent;
    c.fsm.deferred_round = a.deferred;
    c.fsm.draining_input = a.draining;
    c.read_paused = a.read_paused;
    c.peer_closed = a.peer_closed;
    c.write_armed = a.write_armed;
    c.read_pending = a.read_pending;
    if (a.rbuf_bytes) {
        const std::string bytes(a.rbuf_bytes, 'q');
        c.rbuf.write(reinterpret_cast<const uint8_t*>(bytes.data()),
                     bytes.size());
    }
    if (a.wbuf_bytes) {
        const std::string bytes(a.wbuf_bytes, 'a');
        c.wbuf.write(reinterpret_cast<const uint8_t*>(bytes.data()),
                     bytes.size());
    }
}

void check_row(const char* label, const axes& a, const char* expected) {
    connection c;
    apply(c, a);
    const char* why = c.state_violation();
    const bool passed = expected ? (why && std::strcmp(why, expected) == 0)
                                 : (why == nullptr);
    std::string detail;
    if (!passed) {
        detail = why ? std::string("got: ") + why
                     : std::string("expected: ") + expected;
    }
    harness::expect(passed, label, detail);
}

void check_invariant_table() {
    std::printf("== the invariant table ==\n");

    axes a;
    check_row("a healthy live connection is legal", a, nullptr);

    a.write_armed = true;
    a.wbuf_bytes = 8;
    check_row("write interest with buffered output is legal", a, nullptr);
    a = axes{};

    a.peer_closed = true;
    a.write_armed = true;
    check_row("a peer FIN parks with write interest and no read interest",
              a, nullptr);
    a = axes{};

    a.read_paused = true;
    check_row("read backpressure alone is legal", a, nullptr);
    a = axes{};

    a.peer_closed = true;
    a.read_paused = true;
    check_row("read paused after the peer FIN", a,
              "read paused after the peer's FIN");
    a = axes{};

    a.read_paused = true;
    a.read_pending = true;
    check_row("paused connection left on the continuation list", a,
              "paused connection left on the read continuation list");
    a = axes{};

    a.read_paused = true;
    a.draining = true;
    check_row("close drain serving a paused connection", a,
              "close drain serving a read-paused connection");
    a = axes{};

    a.deferred = deferred_kind::peer_closed;
    check_row("peer-closed round owed without a FIN", a,
              "peer-closed round owed without a peer FIN");
    a.peer_closed = true;
    check_row("peer-closed round owed with a FIN", a, nullptr);
    a = axes{};

    a.closing = true;
    a.fin_sent = true;
    check_row("our FIN with an empty wbuf is legal", a, nullptr);
    a.wbuf_bytes = 8;
    check_row("bytes buffered after our FIN", a,
              "bytes buffered after our FIN");
    a = axes{};

    a.fin_sent = true;
    check_row("FIN without a close", a,
              "FIN sent without a closing connection");
    a = axes{};

    a.closing = true;
    a.draining = true;
    a.deferred = deferred_kind::close;
    check_row("a close round while closing is legal", a, nullptr);
    a = axes{};

    a.fd = -1;
    check_row("a detached descriptor is legal", a, nullptr);
    a.is_closed = true;
    check_row("a detached, closed connection is legal", a, nullptr);
    a.write_armed = true;
    check_row("write interest survives the descriptor", a,
              "write interest survives the descriptor");
    a = axes{};
    a.fd = -1;
    a.read_pending = true;
    check_row("read continuation survives the descriptor", a,
              "read continuation survives the descriptor");
    a = axes{};
    a.fd = -1;
    a.is_closed = true;
    a.draining = true;
    check_row("a drain whose handler tore the connection down is legal", a,
              nullptr);
    a = axes{};
    a.fd = -1;
    a.closing = true;
    check_row("closing survives the descriptor", a,
              "closing survives the descriptor");
}

// A decision row owns its setup so it can check the field that row is about,
// instead of comparing a dozen fields the decision never touches. Every row
// also has to leave a legal state behind.
struct decision_row {
    const char* label;
    close_action expected;
    close_action (*act)(connection&, std::string&);
};

void note(std::string& detail, const char* text) {
    if (!detail.empty()) detail += "; ";
    detail += text;
}

void check_decision_table() {
    // close_semantics.cpp already drives close_fsm's action table. These rows
    // keep only the decisions whose result changes a `connection` axis: each
    // one applies the reactor-side effect and requires the resulting combo to
    // be legal. A new legality rule is therefore checked against real
    // decisions, not just against hand-written combinations.
    std::printf("== the close decision table ==\n");

    const decision_row rows[] = {
        {"fresh connection parks into a write drain",
         close_action::drain_wbuf,
         [](connection& c, std::string& detail) {
             const close_action a = c.fsm.on_graceful();
             if (!c.fsm.closing.load(std::memory_order_acquire))
                 note(detail, "closing was not set");
             return a;
         }},
        {"a FINed connection has nothing left to park",
         close_action::none,
         [](connection& c, std::string&) {
             c.fsm.closing.store(true, std::memory_order_relaxed);
             c.fsm.fin_sent = true;
             return c.fsm.on_graceful();
         }},
        {"a request measures its input budget once",
         close_action::drain_input,
         [](connection& c, std::string& detail) {
             const std::string bytes(16, 'q');
             c.rbuf.write(reinterpret_cast<const uint8_t*>(bytes.data()), 16);
             const close_action a = c.fsm.on_request();
             if (c.fsm.close_input_bytes != 16)
                 note(detail, "budget was not rbuf occupancy");
             return a;
         }},
        {"a backlog still inside its budget asks for another round",
         close_action::defer_drain_input,
         [](connection& c, std::string&) {
             const std::string bytes(16, 'q');
             c.rbuf.write(reinterpret_cast<const uint8_t*>(bytes.data()), 16);
             c.fsm.close_input_bytes = 32;
             return c.fsm.on_input_drained(1, 8);
         }},
        {"served input with replies in flight defers the park",
         close_action::defer_park,
         [](connection& c, std::string& detail) {
             c.fsm.close_input_bytes = 0;
             c.pending_bytes.store(8, std::memory_order_relaxed);
             const close_action a = c.fsm.on_input_drained(0, 0);
             if (c.fsm.deferred_round != deferred_kind::close)
                 note(detail, "the close round was not claimed");
             return a;
         }},
        {"the peer-closed round is not reclaimed by a close",
         close_action::none,
         [](connection& c, std::string&) {
             c.peer_closed = true;
             c.fsm.deferred_round = deferred_kind::peer_closed;
             c.pending_bytes.store(8, std::memory_order_relaxed);
             return c.fsm.on_input_drained(0, 0);
         }},
        {"a dead write side aborts instead of waiting for a FIN",
         close_action::close_abort,
         [](connection& c, std::string&) {
             return c.fsm.on_drain_result(drain_outcome::dead);
         }},
        {"a stalled drain leaves writing to EPOLLOUT",
         close_action::none,
         [](connection& c, std::string&) {
             return c.fsm.on_drain_result(drain_outcome::stalled);
         }},
        {"a drained write buffer reaches the close decision",
         close_action::drain_complete,
         [](connection& c, std::string&) {
             return c.fsm.on_drain_result(drain_outcome::drained);
         }},
        {"a peer FIN with output still owed keeps reading off",
         close_action::none,
         [](connection& c, std::string&) {
             c.peer_closed = true;
             const std::string bytes(8, 'a');
             c.wbuf.write(reinterpret_cast<const uint8_t*>(bytes.data()), 8);
             return c.fsm.on_peer_fin();
         }},
        {"queued replies get exactly one peer-closed round",
         close_action::defer_peer_closed,
         [](connection& c, std::string& detail) {
             c.peer_closed = true;
             c.pending_bytes.store(8, std::memory_order_relaxed);
             const close_action a = c.fsm.on_peer_fin();
             if (c.fsm.deferred_round != deferred_kind::peer_closed)
                 note(detail, "the peer-closed round was not claimed");
             return a;
         }},
        {"a closing connection answers a peer FIN with its own FIN",
         close_action::send_fin,
         [](connection& c, std::string&) {
             c.peer_closed = true;
             c.fsm.closing.store(true, std::memory_order_relaxed);
             const close_action a = c.fsm.on_peer_fin();
             if (a == close_action::send_fin) c.fsm.fin_sent = true;
             return a;
         }},
    };

    for (const auto& row : rows) {
        connection c;
        apply(c, axes{});
        std::string detail;
        const close_action got = row.act(c, detail);
        if (got != row.expected) note(detail, "unexpected close_action");
        if (const char* why = c.state_violation()) {
            if (!detail.empty()) detail += "; ";
            detail += std::string("illegal state: ") + why;
        }
        harness::expect(detail.empty(), row.label, detail);
    }
}

#ifndef NDEBUG
// The pure checker is only half the mechanism: the guard has to abort in a
// debug build. A child keeps the expected SIGABRT out of the suite's own
// process. Nothing is started before this point, so there are no reactor
// threads to inherit.
void check_guard_abort() {
    std::printf("== the debug guard ==\n");
    const pid_t pid = ::fork();
    if (pid < 0) {
        harness::expect(false, "the guard probe forks");
        return;
    }
    if (pid == 0) {
        std::freopen("/dev/null", "w", stderr);
        connection c;
        apply(c, axes{});
        c.fd = -1;
        c.write_armed = true;
        c.check_invariants();
        ::_exit(0);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    const bool aborted = WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    harness::expect(aborted, "an illegal combo aborts in a debug build");
}
#endif

}  // namespace

int main() {
    check_invariant_table();
    check_decision_table();
#ifndef NDEBUG
    check_guard_abort();
#endif
    return harness::finish();
}
