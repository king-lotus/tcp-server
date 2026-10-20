// verify/harness.h -- the test-side peer, behind one seam.
//
// Before this file every suite taught itself the same interface: connectTo()
// was defined in ten translation units, <arpa/inet.h> included in twelve, and
// eight programs hand-rolled frame encoding, partial reads, receive timeouts
// and the FIN-versus-RST judgement. A new probe cost ~60 lines of plumbing
// before it could assert anything, and a bug in that plumbing was a bug in
// every suite at once -- the exact failure mode this project keeps paying for.
//
// What sits behind the seam:
//
//   Fixture  owns a started server: ephemeral port, handler, stop on the way
//            out. Callers never see a bind, a thread, or a stop() that has to
//            be paired with a failed start().
//   Peer     owns one connection and speaks frames: send, sendRaw, expectFrame,
//            expectClose(Fin|Rst). "Peer" and not "Client" on purpose -- most of
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
struct Status {
    bool        ok = false;
    std::string why;

    explicit operator bool() const noexcept { return ok; }
};

inline Status ok() { return Status{true, {}}; }
inline Status failed(std::string why) { return Status{false, std::move(why)}; }

// How the far side ended the connection. "Any" is for tests that only care
// that it ended at all.
enum class Close { Fin, Rst, Any };

struct Frame {
    industrial::FrameType type    = industrial::FrameType::DATA;
    std::vector<uint8_t>  payload;
};

// ---------------------------------------------------------------------------
// Assertions: shared printing, per-suite judgement.
// ---------------------------------------------------------------------------

inline int& failureCount() noexcept {
    static int failures = 0;
    return failures;
}

inline void expect(bool passed, const char* what) {
    std::printf("  [%s] %s\n", passed ? "PASS" : "FAIL", what);
    if (!passed) ++failureCount();
}

inline void expect(bool passed, const char* what, const std::string& detail) {
    if (detail.empty()) {
        expect(passed, what);
        return;
    }
    std::printf("  [%s] %s: %s\n", passed ? "PASS" : "FAIL", what, detail.c_str());
    if (!passed) ++failureCount();
}

// The line every suite ends with, and the exit code that goes with it.
inline int finish() {
    if (failureCount() == 0) {
        std::printf("ALL PASS (0 failures)\n");
        return 0;
    }
    std::printf("%d check(s) failed\n", failureCount());
    return 1;
}

// ---------------------------------------------------------------------------
// Fixture: a server that is already listening, and stops itself.
// ---------------------------------------------------------------------------

class Fixture {
public:
    using Handler      = std::function<void(uint64_t, const uint8_t*, size_t)>;
    using CloseHandler = std::function<void(uint64_t, const std::string&, const char*)>;

    explicit Fixture(industrial::MultiReactorServer::Config cfg,
                     Handler handler = Handler(),
                     CloseHandler closeHandler = CloseHandler())
        : cfg_(std::move(cfg)), server_(cfg_) {
        if (handler) server_.setMessageHandler(std::move(handler));
        if (closeHandler) server_.setCloseHandler(std::move(closeHandler));
        started_ = server_.start();
    }

    ~Fixture() { stop(); }

    // Idempotent, and callable early: a suite that wants to assert about a
    // stopped server has to stop it before it looks.
    void stop() noexcept {
        if (started_ && !stopped_) {
            server_.stop();
            stopped_ = true;
        }
    }

    Fixture(const Fixture&)            = delete;
    Fixture& operator=(const Fixture&) = delete;

    bool started() const noexcept { return started_; }

    // Only meaningful once started(): with cfg.port == 0 the kernel picks, and
    // reading it back is the only way to learn what it picked.
    uint16_t port() const noexcept { return server_.port(); }

    industrial::MultiReactorServer&       server() noexcept { return server_; }
    const industrial::ServerStats&        stats() const noexcept { return server_.stats(); }

private:
    industrial::MultiReactorServer::Config cfg_;
    industrial::MultiReactorServer         server_;
    bool                                   started_ = false;
    bool                                   stopped_ = false;
};

// ---------------------------------------------------------------------------
// Peer: one connection, frame at a time.
// ---------------------------------------------------------------------------

class Peer {
public:
    Peer() = default;

    ~Peer() { close(); }

    Peer(const Peer&)            = delete;
    Peer& operator=(const Peer&) = delete;

    Peer(Peer&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

    Peer& operator=(Peer&& other) noexcept {
        if (this != &other) {
            close();
            fd_       = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    // Blocking connect to loopback. The timeout is a ceiling, not a schedule:
    // on loopback it either succeeds at once or the port is not listening.
    Status connect(uint16_t port, int timeoutMs = 2000) {
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
        (void)timeoutMs;
        fd_ = fd;
        return ok();
    }

    Status send(industrial::FrameType type, const void* payload, size_t len) {
        if (fd_ < 0) return failed("send: not connected");
        if (len > industrial::MAX_FRAME_PAYLOAD) return failed("send: payload too large");
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + len);
        industrial::encodeFrameInto(f.data(), type, payload, len);
        return sendRaw(f.data(), f.size());
    }

    // For the cases that must violate the protocol on purpose: a bad magic
    // number, a length that exceeds MAX_FRAME_PAYLOAD, half a header. Naming it
    // "raw" keeps the intent visible instead of handing out the socket.
    Status sendRaw(const void* bytes, size_t len) {
        if (fd_ < 0) return failed("sendRaw: not connected");
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

    Status expectFrame(Frame* out, int timeoutMs = 500) {
        if (fd_ < 0) return failed("expectFrame: not connected");
        if (out == nullptr) return failed("expectFrame: null out");

        std::vector<uint8_t> buf;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
        for (;;) {
            if (buf.size() >= industrial::FRAME_HEADER_SIZE) {
                const uint32_t len = (static_cast<uint32_t>(buf[4]) << 24)
                                   | (static_cast<uint32_t>(buf[5]) << 16)
                                   | (static_cast<uint32_t>(buf[6]) <<  8)
                                   |  static_cast<uint32_t>(buf[7]);
                if (buf.size() >= industrial::FRAME_HEADER_SIZE + len) {
                    out->type = static_cast<industrial::FrameType>(
                        (static_cast<uint16_t>(buf[2]) << 8) | buf[3]);
                    out->payload.assign(
                        buf.begin() + industrial::FRAME_HEADER_SIZE,
                        buf.begin() + industrial::FRAME_HEADER_SIZE + len);
                    return ok();
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return failed("expectFrame: timeout");
            }
            uint8_t       tmp[512];
            const ssize_t r = ::recv(fd_, tmp, sizeof(tmp), MSG_DONTWAIT);
            if (r > 0) {
                buf.insert(buf.end(), tmp, tmp + r);
                continue;
            }
            if (r == 0) return failed("expectFrame: peer closed first (FIN)");
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("expectFrame: recv ") + std::strerror(errno));
        }
    }

    // Waits for the far side to end the connection, and says how it did. A
    // frame that arrives first is discarded: the question is about the ending.
    //
    // The answer is remembered. Once a reset has been reported, further recv()
    // calls on that socket return 0 -- which looks exactly like an orderly FIN,
    // so a second call would relabel a reset connection as a clean one.
    Status expectClose(Close want, int timeoutMs = 1000) {
        if (fd_ < 0) return failed("expectClose: not connected");
        if (closeSeen_) return closedAs(want, closeKind_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
        for (;;) {
            char          tmp[512];
            const ssize_t r = ::recv(fd_, tmp, sizeof(tmp), MSG_DONTWAIT);
            if (r > 0) continue;
            if (r == 0) return rememberClosed(want, Close::Fin);
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) return rememberClosed(want, Close::Rst);
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return failed("expectClose: timeout, no close");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("expectClose: recv ") + std::strerror(errno));
        }
    }

    // Reads until the far side ends the connection, keeping every byte that
    // arrived before it did, and reports which ending it was.
    //
    // This is the primitive close_semantics needs and expectClose cannot give:
    // there, "which bytes arrived" and "how did it end" have to come from the
    // same read, because a reply that was queued and then refused looks exactly
    // like a reply that was never produced unless you count the bytes.
    Status readToEnd(std::vector<uint8_t>* out, Close* end, int timeoutMs = 2000) {
        if (out != nullptr) out->clear();
        if (end != nullptr) *end = Close::Any;
        if (fd_ < 0) return failed("readToEnd: not connected");

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
        for (;;) {
            uint8_t       buf[4096];
            const ssize_t r = ::recv(fd_, buf, sizeof(buf), MSG_DONTWAIT);
            if (r > 0) {
                if (out != nullptr) out->insert(out->end(), buf, buf + r);
                continue;
            }
            if (r == 0) {
                if (end != nullptr) *end = Close::Fin;
                closeSeen_ = true;
                closeKind_ = Close::Fin;
                return ok();
            }
            if (errno == EINTR) continue;
            if (errno == ECONNRESET) {
                if (end != nullptr) *end = Close::Rst;
                closeSeen_ = true;
                closeKind_ = Close::Rst;
                return ok();
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return failed("readToEnd: timeout before the far side ended");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            return failed(std::string("readToEnd: recv ") + std::strerror(errno));
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
    static const char* nameOf(Close c) noexcept {
        switch (c) {
            case Close::Fin: return "FIN";
            case Close::Rst: return "RST";
            case Close::Any: return "any";
        }
        return "?";
    }

    static Status closedAs(Close want, Close saw) {
        if (want == Close::Any || want == saw) return ok();
        return failed(std::string("expected ") + nameOf(want) + ", saw " + nameOf(saw));
    }

    Status rememberClosed(Close want, Close saw) {
        closeSeen_ = true;
        closeKind_ = saw;
        return closedAs(want, saw);
    }

    int   fd_        = -1;
    bool  closeSeen_ = false;
    Close closeKind_ = Close::Any;
};

// ---------------------------------------------------------------------------
// Fixture construction helper: a Config that no timer can end on its own, and
// a port the kernel picks.
// ---------------------------------------------------------------------------

inline industrial::MultiReactorServer::Config quietConfig(int reactors = 1) {
    industrial::MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.numReactors   = reactors;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;
    cfg.closeLingerMs = 5000;
    return cfg;
}

}  // namespace harness
