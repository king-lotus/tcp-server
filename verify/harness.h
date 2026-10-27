// verify/harness.h -- the test-side peer, behind one seam.
//
// Before this file every suite taught itself the same interface: connect_to()
// was defined in ten translation units, <arpa/inet.h> included in twelve, and
// eight programs hand-rolled frame encoding, partial reads, receive timeouts
// and the FIN-versus-RST judgement. A new probe cost ~60 lines of plumbing
// before it could assert anything, and a bug in that plumbing was a bug in
// every suite at once -- the exact failure mode this project keeps paying for.
//
// What sits behind the seam:
//
//   fixture  owns a started server: ephemeral port, handler, stop on the way
//            out. Callers never see a bind, a thread, or a stop() that has to
//            be paired with a failed start().
//   peer     owns one connection and speaks frames: send, send_raw, expect_frame,
//            expect_close(fin|rst). "peer" and not "Client" on purpose -- most of
//            these suites exist to make the far side misbehave, and a module
//            named Client invites well-behaved assumptions.
//
// What deliberately stays outside: assertions. harness::expect prints and
// counts, but which invariant to assert is written in the suite, where it can
// be read. Printing is shared; judgement is not.
//
// Linux only, like everything under verify/.

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <utility>
#include <thread>
#include <utility>
#include <vector>

#include "tcp-server-mt.h"

namespace harness {

// An operation that can fail, with the reason it did. No exceptions: the
// library is noexcept throughout and a suite that unwinds would report a crash
// where it meant a failed assertion.
struct status {
    bool        ok = false;
    std::string why;

    explicit operator bool() const noexcept { return ok; }
};

inline status ok() { return status{true, {}}; }
inline status failed(std::string why) { return status{false, std::move(why)}; }

// How the far side ended the connection. "any" is for tests that only care
// that it ended at all.
enum class close_kind { fin, rst, any };

struct frame {
    industrial::frame_type type    = industrial::frame_type::DATA;
    std::vector<uint8_t>  payload;
};

// ---------------------------------------------------------------------------
// Assertions: shared printing, per-suite judgement.
// ---------------------------------------------------------------------------

inline int& failure_count() noexcept {
    static int failures = 0;
    return failures;
}

inline void expect(bool passed, const char* what) {
    std::printf("  [%s] %s\n", passed ? "PASS" : "FAIL", what);
    if (!passed) ++failure_count();
}

inline void expect(bool passed, const char* what, const std::string& detail) {
    if (detail.empty()) {
        expect(passed, what);
        return;
    }
    std::printf("  [%s] %s: %s\n", passed ? "PASS" : "FAIL", what, detail.c_str());
    if (!passed) ++failure_count();
}

// The line every suite ends with, and the exit code that goes with it.
inline int finish() {
    if (failure_count() == 0) {
        std::printf("ALL PASS (0 failures)\n");
        return 0;
    }
    std::printf("%d check(s) failed\n", failure_count());
    return 1;
}

// ---------------------------------------------------------------------------
// fixture: a server that is already listening, and stops itself.
// ---------------------------------------------------------------------------

class fixture {
public:
    using message_handler      = std::function<void(uint64_t, const uint8_t*, size_t)>;
    using close_handler = std::function<void(uint64_t, const std::string&, const char*)>;
    using conn_state_handler = std::function<void(uint64_t, const std::string&, bool)>;

    explicit fixture(industrial::multi_reactor_server::config cfg,
                     message_handler handler = message_handler(),
                     close_handler close_cb = close_handler(),
                     conn_state_handler conn_state = conn_state_handler())
        : cfg_(std::move(cfg)), server_(cfg_) {
        if (handler) server_.set_message_handler(std::move(handler));
        if (close_cb) server_.set_close_handler(std::move(close_cb));
        if (conn_state) server_.set_conn_state_handler(std::move(conn_state));
        started_ = server_.start();
    }

    ~fixture() { stop(); }

    // Idempotent, and callable early: a suite that wants to assert about a
    // stopped server has to stop it before it looks.
    void stop() noexcept {
        if (started_ && !stopped_) {
            server_.stop();
            stopped_ = true;
        }
    }

    fixture(const fixture&)            = delete;
    fixture& operator=(const fixture&) = delete;

    bool started() const noexcept { return started_; }

    // Only meaningful once started(): with cfg.port == 0 the kernel picks, and
    // reading it back is the only way to learn what it picked.
    uint16_t port() const noexcept { return server_.port(); }

    industrial::multi_reactor_server&       server() noexcept { return server_; }
    const industrial::server_stats&        stats() const noexcept { return server_.stats(); }

private:
    industrial::multi_reactor_server::config cfg_;
    industrial::multi_reactor_server         server_;
    bool                                   started_ = false;
    bool                                   stopped_ = false;
};

// ---------------------------------------------------------------------------
// peer: one connection, frame at a time.
// ---------------------------------------------------------------------------

class peer {
public:
    peer() = default;

    ~peer() { close(); }

    peer(const peer&)            = delete;
    peer& operator=(const peer&) = delete;

    peer(peer&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

    peer& operator=(peer&& other) noexcept {
        if (this != &other) {
            close();
            fd_       = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    // Blocking connect to loopback. The timeout is a ceiling, not a schedule:
    // on loopback it either succeeds at once or the port is not listening.
    status connect(uint16_t port, int timeout_ms = 2000) {
        close();
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return failed(std::string("socket: ") + std::strerror(errno));

        struct sockaddr_in sa{};
        sa.sin_family      = AF_INET;
        sa.sin_port        = htons(port);
        sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
            const std::string why = std::string("connect: ") + std::strerror(errno);
            ::close(fd);
            return failed(why);
        }
        (void)timeout_ms;
        fd_ = fd;
        return ok();
    }

    status send(industrial::frame_type type, const void* payload, size_t len) {
        if (fd_ < 0) return failed("send: not connected");
        if (len > industrial::MAX_FRAME_PAYLOAD) return failed("send: payload too large");
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + len);
        industrial::encode_frame_into(f.data(), type, payload, len);
        return send_raw(f.data(), f.size());
    }

    // For the cases that must violate the protocol on purpose: a bad magic
    // number, a length that exceeds MAX_FRAME_PAYLOAD, half a header. Naming it
    // "raw" keeps the intent visible instead of handing out the socket.
    status send_raw(const void* bytes, size_t len) {
        if (fd_ < 0) return failed("send_raw: not connected");
        const uint8_t* b   = static_cast<const uint8_t*>(bytes);
        size_t         off = 0;
        while (off < len) {
            const ssize_t k = ::send(fd_, b + off, len - off, MSG_NOSIGNAL);
            if (k <= 0) {
                if (errno == EINTR) continue;
                return failed(std::string("send: ") + std::strerror(errno));
            }
            off += static_cast<size_t>(k);
        }
        return ok();
    }

    status expect_frame(frame* out, int timeout_ms = 500) {
        if (fd_ < 0) return failed("expect_frame: not connected");
        if (out == nullptr) return failed("expect_frame: null out");

        std::vector<uint8_t> buf;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (;;) {
            if (buf.size() >= industrial::FRAME_HEADER_SIZE) {
                const uint32_t len = (static_cast<uint32_t>(buf[4]) << 24)
                                   | (static_cast<uint32_t>(buf[5]) << 16)
                                   | (static_cast<uint32_t>(buf[6]) <<  8)
                                   |  static_cast<uint32_t>(buf[7]);
                if (buf.size() >= industrial::FRAME_HEADER_SIZE + len) {
                    out->type = static_cast<industrial::frame_type>(
                        (static_cast<uint16_t>(buf[2]) << 8) | buf[3]);
                    out->payload.assign(
                        buf.begin() + industrial::FRAME_HEADER_SIZE,
                        buf.begin() + industrial::FRAME_HEADER_SIZE + len);
                    return ok();
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return failed("expect_frame: timeout");
            }
            uint8_t       tmp[512];
            const ssize_t r = ::recv(fd_, tmp, sizeof(tmp), MSG_DONTWAIT);
            if (r > 0) {
                buf.insert(buf.end(), tmp, tmp + r);
                continue;
            }
            if (r == 0) return failed("expect_frame: peer closed first (FIN)");
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("expect_frame: recv ") + std::strerror(errno));
        }
    }

    // Waits for the far side to end the connection, and says how it did. A
    // frame that arrives first is discarded: the question is about the ending.
    //
    // The answer is remembered. Once a reset has been reported, further recv()
    // calls on that socket return 0 -- which looks exactly like an orderly FIN,
    // so a second call would relabel a reset connection as a clean one.
    status expect_close(close_kind want, int timeout_ms = 1000) {
        if (fd_ < 0) return failed("expect_close: not connected");
        if (close_seen_) return closed_as(want, close_kind_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (;;) {
            char          tmp[512];
            const ssize_t r = ::recv(fd_, tmp, sizeof(tmp), MSG_DONTWAIT);
            if (r > 0) continue;
            if (r == 0) return remember_closed(want, close_kind::fin);
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) return remember_closed(want, close_kind::rst);
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return failed("expect_close: timeout, no close");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("expect_close: recv ") + std::strerror(errno));
        }
    }

    // Reads until the far side ends the connection, keeping every byte that
    // arrived before it did, and reports which ending it was.
    //
    // This is the primitive close_semantics needs and expect_close cannot give:
    // there, "which bytes arrived" and "how did it end" have to come from the
    // same read, because a reply that was queued and then refused looks exactly
    // like a reply that was never produced unless you count the bytes.
    status read_to_end(std::vector<uint8_t>* out, close_kind* end, int timeout_ms = 2000) {
        if (out != nullptr) out->clear();
        if (end != nullptr) *end = close_kind::any;
        if (fd_ < 0) return failed("read_to_end: not connected");

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (;;) {
            uint8_t       buf[4096];
            const ssize_t r = ::recv(fd_, buf, sizeof(buf), MSG_DONTWAIT);
            if (r > 0) {
                if (out != nullptr) out->insert(out->end(), buf, buf + r);
                continue;
            }
            if (r == 0) {
                if (end != nullptr) *end = close_kind::fin;
                close_seen_ = true;
                close_kind_ = close_kind::fin;
                return ok();
            }
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) {
                if (end != nullptr) *end = close_kind::rst;
                close_seen_ = true;
                close_kind_ = close_kind::rst;
                return ok();
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return failed("read_to_end: timeout before the far side ended");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("read_to_end: recv ") + std::strerror(errno));
        }
    }

    int  fd() const noexcept { return fd_; }
    bool connected() const noexcept { return fd_ >= 0; }

    void close() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    static const char* name_of(close_kind c) noexcept {
        switch (c) {
            case close_kind::fin: return "FIN";
            case close_kind::rst: return "RST";
            case close_kind::any: return "any";
        }
        return "?";
    }

    static status closed_as(close_kind want, close_kind saw) {
        if (want == close_kind::any || want == saw) return ok();
        return failed(std::string("expected ") + name_of(want) + ", saw " + name_of(saw));
    }

    status remember_closed(close_kind want, close_kind saw) {
        close_seen_ = true;
        close_kind_ = saw;
        return closed_as(want, saw);
    }

    int   fd_        = -1;
    bool  close_seen_ = false;
    close_kind close_kind_ = close_kind::any;
};

// ---------------------------------------------------------------------------
// fixture construction helper: a config that no timer can end on its own, and
// a port the kernel picks.
// ---------------------------------------------------------------------------

// A port the kernel is willing to hand out right now. The gap between this call
// and the bind is real, so this is only for the tests that need the number before
// the server exists: a forked child, a second listener, a probe that must run
// before start().
inline uint16_t free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (::bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(sa);
    if (::getsockname(fd, (struct sockaddr*)&sa, &len) < 0) {
        ::close(fd);
        return 0;
    }
    const uint16_t port = ntohs(sa.sin_port);
    ::close(fd);
    return port;
}

inline industrial::multi_reactor_server::config quiet_config(int reactors = 1) {
    industrial::multi_reactor_server::config cfg;
    cfg.bind_addr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.num_reactors   = reactors;
    cfg.tick_ms        = 50;
    cfg.idle_timeout_ms = 60000;
    cfg.close_linger_ms = 5000;
    return cfg;
}

}  // namespace harness
