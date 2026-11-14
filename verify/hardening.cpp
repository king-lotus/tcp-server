// verify/hardening.cpp -- the admission and identity guarantees added by the
// architecture-hardening pass.
//
//   A. connection ids are never reused, and the id-keyed event dispatch never
//      acts on a stale event (churn: connect/serve/close/reconnect);
//   B. a throwing close callback is contained and counted instead of taking
//      the process down, and the server keeps serving;
//   C. the global buffer budget refuses an admission beyond its byte ceiling
//      and gives the reservation back when a connection closes;
//   D. the per-IP cap refuses a second connection from one address and admits
//      it again after the first is gone;
//   E. SO_REUSEPORT is opt-in: a duplicate bind fails by default and succeeds
//      only when the second server asks for it.
//
// Linux only. Exit code 0 = all cases passed.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "harness.h"
#include "tcp-server-mt.h"

using industrial::frame_type;
using industrial::multi_reactor_server;

namespace {

bool wait_for(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

multi_reactor_server::config base_config() {
    multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port           = 0;
    cfg.num_reactors   = 1;
    cfg.tick_ms        = 20;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 1000;
    return cfg;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- A: ids are unique and stale events cannot alias -------------------
    {
        std::printf("A. id churn: connect, serve, close, reconnect\n");
        std::vector<uint64_t> seen;
        std::atomic<multi_reactor_server*> srv{nullptr};
        harness::fixture f(
            base_config(),
            [&](uint64_t cid, const uint8_t* p, size_t n) {
                if (auto* s = srv.load()) (void)s->send_to(cid, p, n);
            });
        srv.store(&f.server());
        if (!f.started()) {
            harness::expect(false, "start() ok");
        } else {
            constexpr int kRounds = 100;
            bool ids_increasing = true;
            for (int i = 0; i < kRounds; ++i) {
                harness::peer p;
                if (!p.connect(f.port())) {
                    harness::expect(false, "client connected");
                    break;
                }
                (void)p.send(frame_type::DATA, "x", 1);
                harness::frame echo;
                if (!p.expect_frame(&echo, 2000)) {
                    harness::expect(false, "echo arrived");
                    break;
                }
                const std::vector<uint64_t> ids = f.server().connection_ids();
                if (ids.size() == 1) {
                    if (!seen.empty() && ids[0] <= seen.back())
                        ids_increasing = false;
                    seen.push_back(ids[0]);
                }
                p.close();
                if (!wait_for(2000, [&] {
                        return f.server().connection_count() == 0;
                    })) {
                    harness::expect(false, "connection closed before the next round");
                    break;
                }
            }
            std::printf("      %zu ids recorded over %d rounds\n",
                        seen.size(), kRounds);
            harness::expect(seen.size() == kRounds,
                            "every round produced a connection id");
            harness::expect(ids_increasing,
                            "connection ids never repeat");
            harness::expect(wait_for(2000, [&] {
                                return f.stats().accepted.load() ==
                                       f.stats().closed.load();
                            }), "accepted == closed after the churn");
            harness::expect(f.stats().aborted.load() == 0,
                            "no orderly close was misread as an abort");
        }
    }

    // ---- B: a throwing close callback is contained -------------------------
    {
        std::printf("B. a throwing close callback does not kill the server\n");
        std::atomic<int> closed{0};
        harness::fixture f(
            base_config(),
            [&](uint64_t cid, const uint8_t* p, size_t n) {
                (void)f.server().send_to(cid, p, n);
            },
            [&](uint64_t, const std::string&, const char*) {
                closed.fetch_add(1);
                throw std::runtime_error("application close handler");
            });
        if (!f.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer p;
            (void)p.connect(f.port());
            (void)p.send(frame_type::DATA, "x", 1);
            p.close();
            harness::expect(wait_for(2000, [&] { return closed.load() >= 1; }),
                            "the close callback ran");
            harness::expect(f.server().is_running(),
                            "the server survived the throw");
            harness::expect(wait_for(2000, [&] {
                                return f.stats().callback_exceptions.load() >= 1;
                            }), "callback_exceptions counted it");

            harness::peer again;
            harness::expect(static_cast<bool>(again.connect(f.port())),
                            "a new connection is still accepted");
            (void)again.send(frame_type::DATA, "y", 1);
            harness::frame echo;
            harness::expect(static_cast<bool>(again.expect_frame(&echo, 2000)),
                            "and the server still answers");
        }
    }

    // ---- C: the global buffer budget ---------------------------------------
    {
        std::printf("C. the buffer budget refuses past its byte ceiling\n");
        multi_reactor_server::config cfg = base_config();
        cfg.max_connections = 10;
        const size_t per_conn = cfg.per_connection_buffer_bytes();
        cfg.max_total_buffer_bytes = 2 * per_conn;
        std::atomic<int> closed{0};
        harness::fixture f(
            cfg,
            [&](uint64_t cid, const uint8_t* p, size_t n) {
                (void)f.server().send_to(cid, p, n);
            },
            [&](uint64_t, const std::string&, const char*) {
                closed.fetch_add(1);
            });
        if (!f.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer a, b, c;
            (void)a.connect(f.port());
            (void)b.connect(f.port());
            (void)c.connect(f.port());
            harness::expect(wait_for(3000, [&] {
                                return f.server().connection_count() == 2;
                            }), "two connections fit the budget");
            harness::expect(wait_for(3000, [&] {
                                return f.stats().buffer_budget_rejections.load() >= 1;
                            }), "the third was refused by the byte ceiling");
            harness::expect(f.stats().rejected.load() >= 1,
                            "and counted as rejected");
            c.close();

            const int before = closed.load();
            a.close();
            harness::expect(wait_for(2000, [&] {
                                return closed.load() > before;
                            }), "closing a connection released its reservation");
            harness::peer d;
            harness::expect(static_cast<bool>(d.connect(f.port())),
                            "the freed slot admits a new connection");
            harness::expect(wait_for(2000, [&] {
                                return f.server().connection_count() == 2;
                            }), "the new connection is live");
        }
    }

    // ---- D: the per-IP cap -------------------------------------------------
    {
        std::printf("D. the per-IP cap refuses and later re-admits\n");
        multi_reactor_server::config cfg = base_config();
        cfg.max_connections_per_ip = 1;
        std::atomic<int> closed{0};
        harness::fixture f(
            cfg,
            [&](uint64_t cid, const uint8_t* p, size_t n) {
                (void)f.server().send_to(cid, p, n);
            },
            [&](uint64_t, const std::string&, const char*) {
                closed.fetch_add(1);
            });
        if (!f.started()) {
            harness::expect(false, "start() ok");
        } else {
            harness::peer a, b;
            (void)a.connect(f.port());
            harness::expect(wait_for(2000, [&] {
                                return f.server().connection_count() == 1;
                            }), "the first connection is admitted");
            (void)b.connect(f.port());
            harness::expect(wait_for(3000, [&] {
                                return f.stats().ip_rejections.load() >= 1;
                            }), "the second connection from the same IP is refused");
            harness::expect(f.server().connection_count() == 1,
                            "only one connection from the address is live");
            b.close();

            const int before = closed.load();
            a.close();
            harness::expect(wait_for(2000, [&] {
                                return closed.load() > before;
                            }), "the first connection closed");
            harness::peer c;
            (void)c.connect(f.port());
            harness::expect(wait_for(2000, [&] {
                                return f.server().connection_count() == 1;
                            }), "the slot is available again");
        }
    }

    // ---- E: SO_REUSEPORT is opt-in -----------------------------------------
    {
        std::printf("E. SO_REUSEPORT is off unless asked for\n");
        multi_reactor_server::config cfg = base_config();
        cfg.port = harness::free_port();
        cfg.reuse_port = false;
        multi_reactor_server first(cfg);
        harness::expect(first.start(), "the first server starts");
        if (first.is_running()) {
            multi_reactor_server second(cfg);
            harness::expect(!second.start(),
                            "a duplicate bind is refused by default");
            if (second.is_running()) second.stop();
            first.stop();
        }

        cfg.port = harness::free_port();
        cfg.reuse_port = true;
        multi_reactor_server shared_a(cfg);
        multi_reactor_server shared_b(cfg);
        harness::expect(shared_a.start(), "the first reuse_port server starts");
        harness::expect(shared_b.start(),
                        "a second server may share the port when it opts in");
        if (shared_a.is_running()) shared_a.stop();
        if (shared_b.is_running()) shared_b.stop();
    }

    return harness::finish();
}
