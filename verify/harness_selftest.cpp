// verify/harness_selftest.cpp -- tests the test harness.
//
// Every suite runs its assertions through verify/harness.h. If Peer's framing,
// timeout or FIN-versus-RST judgement is wrong, every suite that uses it is
// wrong in the same direction and reports green while doing it -- which is the
// one failure mode this project has paid for repeatedly.
//
// So the harness gets its own suite. It does not start a server: it drives Peer
// against a bare listening socket whose behaviour this file controls, which is
// the only way to make it lie on demand.
//
//   A. send() emits a frame the far side can decode (magic, type, length, body)
//   B. expectFrame() reassembles a frame that arrives split across writes
//   C. expectClose(Fin) recognises an orderly close
//   D. expectClose(Rst) recognises a reset, and does not call it a FIN
//   E. expectFrame() times out on silence and says so
//   F. sendRaw() writes the bytes it was given, unchanged
//
// Exit code 0 = the harness distinguishes all six.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "harness.h"

using industrial::FrameType;

namespace {

// A listening socket this file drives by hand. One connection per case, so a
// case that ends the connection cannot affect the next one.
struct RawServer {
    int      listenFd = -1;
    uint16_t portNum  = 0;

    bool start() {
        listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listenFd < 0) return false;
        int one = 1;
        ::setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        struct sockaddr_in sa{};
        sa.sin_family      = AF_INET;
        sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        sa.sin_port        = 0;
        if (::bind(listenFd, (struct sockaddr*)&sa, sizeof(sa)) < 0) return false;
        if (::listen(listenFd, 4) < 0) return false;
        socklen_t sl = sizeof(sa);
        if (::getsockname(listenFd, (struct sockaddr*)&sa, &sl) < 0) return false;
        portNum = ntohs(sa.sin_port);
        return true;
    }

    int acceptOne() const { return ::accept(listenFd, nullptr, nullptr); }

    ~RawServer() {
        if (listenFd >= 0) ::close(listenFd);
    }
};

// Reads up to n bytes, or gives up after ms. Returns how many arrived.
size_t readAtMost(int fd, void* dst, size_t n, int ms = 1000) {
    uint8_t*      out = static_cast<uint8_t*>(dst);
    const auto    end = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(ms);
    size_t        got = 0;
    while (got < n) {
        if (std::chrono::steady_clock::now() >= end) break;
        const ssize_t r = ::recv(fd, out + got, n - got, MSG_DONTWAIT);
        if (r > 0) {
            got += static_cast<size_t>(r);
            continue;
        }
        if (r == 0) break;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            continue;
        }
        break;
    }
    return got;
}

bool sendAll(int fd, const void* p, size_t n) {
    const uint8_t* b   = static_cast<const uint8_t*>(p);
    size_t         off = 0;
    while (off < n) {
        const ssize_t k = ::send(fd, b + off, n - off, MSG_NOSIGNAL);
        if (k <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

void caseSendFraming() {
    std::printf("A. send() frames the payload the far side expects\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }

    const char body[] = "abc";
    (void)peer.send(FrameType::DATA, body, 3);

    uint8_t wire[32];
    const size_t n = readAtMost(fd, wire, 8 + 3);
    harness::expect(n == 8 + 3, "the frame arrived whole",
                    std::to_string(n) + " of 11 bytes");
    if (n == 8 + 3) {
        const uint16_t magic = (static_cast<uint16_t>(wire[0]) << 8) | wire[1];
        const uint16_t type  = (static_cast<uint16_t>(wire[2]) << 8) | wire[3];
        const uint32_t len   = (static_cast<uint32_t>(wire[4]) << 24)
                             | (static_cast<uint32_t>(wire[5]) << 16)
                             | (static_cast<uint32_t>(wire[6]) <<  8)
                             |  static_cast<uint32_t>(wire[7]);
        harness::expect(magic == industrial::FRAME_MAGIC, "magic is the protocol's");
        harness::expect(type == static_cast<uint16_t>(FrameType::DATA), "type is DATA");
        harness::expect(len == 3, "length is the payload's");
        harness::expect(std::memcmp(wire + 8, body, 3) == 0, "payload is unchanged");
    }

    peer.close();
    ::close(fd);
}

void caseSplitFrame() {
    std::printf("B. expectFrame() reassembles a frame split across writes\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }

    uint8_t frame[16];
    industrial::encodeFrameInto(frame, FrameType::PONG, "xy", 2);
    (void)sendAll(fd, frame, 4);                                  // half a header
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    (void)sendAll(fd, frame + 4, 6);                              // rest of header
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    (void)sendAll(fd, frame + 10, 2);                             // payload

    harness::Frame got;
    const harness::Status st = peer.expectFrame(&got, 1000);
    harness::expect(static_cast<bool>(st), "a frame was received", st.why);
    if (st) {
        harness::expect(got.type == FrameType::PONG, "type survived the split");
        harness::expect(got.payload.size() == 2 &&
                            got.payload[0] == 'x' && got.payload[1] == 'y',
                        "payload survived the split");
    }

    peer.close();
    ::close(fd);
}

void caseFin() {
    std::printf("C. expectClose(Fin) recognises an orderly close\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }
    ::close(fd);                                                  // orderly FIN

    const harness::Status st = peer.expectClose(harness::Close::Fin, 1000);
    harness::expect(static_cast<bool>(st), "FIN was reported", st.why);
    peer.close();
}

void caseRst() {
    std::printf("D. expectClose(Rst) recognises a reset\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }

    struct linger lg{};
    lg.l_onoff  = 1;
    lg.l_linger = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    ::close(fd);                                                  // RST

    const harness::Status st = peer.expectClose(harness::Close::Rst, 1000);
    harness::expect(static_cast<bool>(st), "RST was reported", st.why);

    // And it must not mistake the reset for a FIN: the same wait asked for FIN
    // has to fail, because the connection is already gone.
    const harness::Status asFin = peer.expectClose(harness::Close::Fin, 100);
    harness::expect(!asFin, "and it is not reported as a FIN");
    peer.close();
}

void caseTimeout() {
    std::printf("E. expectFrame() times out on silence\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }

    harness::Frame f;
    const harness::Status st = peer.expectFrame(&f, 100);
    harness::expect(!st, "no frame was invented");
    harness::expect(st.why.find("timeout") != std::string::npos,
                    "and the reason says timeout", st.why);

    ::close(fd);
    peer.close();
}

void caseRawBytes() {
    std::printf("F. sendRaw() writes the bytes verbatim\n");
    RawServer rs;
    if (!rs.start()) { harness::expect(false, "raw listener bound"); return; }

    harness::Peer peer;
    harness::expect(static_cast<bool>(peer.connect(rs.portNum)), "peer connected");
    const int fd = rs.acceptOne();
    if (fd < 0) { harness::expect(false, "raw side accepted"); return; }

    const uint8_t junk[3] = {0xde, 0xad, 0xbe};                   // not a frame
    (void)peer.sendRaw(junk, sizeof(junk));

    uint8_t got[3] = {0, 0, 0};
    const size_t n = readAtMost(fd, got, 3);
    harness::expect(n == 3 && std::memcmp(got, junk, 3) == 0,
                    "the three bytes arrived unchanged");

    ::close(fd);
    peer.close();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    caseSendFraming();
    caseSplitFrame();
    caseFin();
    caseRst();
    caseTimeout();
    caseRawBytes();
    std::printf("\n");
    return harness::finish();
}
