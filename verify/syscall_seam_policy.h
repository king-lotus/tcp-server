// verify/syscall_seam_policy.h -- the test half of the syscall seam.
//
// Include this BEFORE tcp-server-mt.h (or harness.h, which includes it), and
// define TCP_SERVER_SYSCALL_POLICY to industrial::seam_policy first. The header
// then aliases syscall_policy to this type in every translation unit that
// makes up the test binary -- one policy per binary, never per TU.
//
// Each knob is a one-shot errno. A zero value forwards to the real syscall.
// Setting a knob from a test thread is a release store; the reactor thread
// consumes it with exchange(0, acq_rel), so exactly one call sees the fault
// and all later calls behave normally. This is what makes a failure test
// deterministic without a fork, a lowered rlimit or a real EPIPE window.
//
// errno is set by the policy itself: the production code reads errno right
// after the seam call, exactly as it did after the direct syscall.

#pragma once

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>

namespace industrial {

struct seam_policy {
    static std::atomic<int> recv_errno;
    static std::atomic<int> sendmsg_errno;
    static std::atomic<int> sendmsg_short_bytes;
    static std::atomic<int> shutdown_errno;
    static std::atomic<int> epoll_ctl_errno;
    static std::atomic<int> epoll_ctl_op;
    static std::atomic<int> accept4_errno;

    // How many calls of each kind have gone through the seam. Tests use these
    // to prove a knob was actually consumed rather than merely armed.
    static std::atomic<uint64_t> recv_calls;
    static std::atomic<uint64_t> sendmsg_calls;
    static std::atomic<uint64_t> shutdown_calls;
    static std::atomic<uint64_t> epoll_ctl_calls;
    static std::atomic<uint64_t> accept4_calls;

    static void reset() noexcept {
        recv_errno.store(0);
        sendmsg_errno.store(0);
        sendmsg_short_bytes.store(0);
        shutdown_errno.store(0);
        epoll_ctl_errno.store(0);
        epoll_ctl_op.store(-1);
        accept4_errno.store(0);
        recv_calls.store(0);
        sendmsg_calls.store(0);
        shutdown_calls.store(0);
        epoll_ctl_calls.store(0);
        accept4_calls.store(0);
    }

    static ssize_t recv(int fd, void* buf, size_t len, int flags) noexcept {
        recv_calls.fetch_add(1, std::memory_order_relaxed);
        const int e = recv_errno.exchange(0, std::memory_order_acq_rel);
        if (e != 0) {
            errno = e;
            return -1;
        }
        return ::recv(fd, buf, len, flags);
    }

    static ssize_t sendmsg(int fd, const struct msghdr* msg,
                           int flags) noexcept {
        sendmsg_calls.fetch_add(1, std::memory_order_relaxed);
        const int e = sendmsg_errno.exchange(0, std::memory_order_acq_rel);
        if (e != 0) {
            errno = e;
            return -1;
        }
        const int short_n =
            sendmsg_short_bytes.exchange(0, std::memory_order_acq_rel);
        if (short_n > 0) {
            struct msghdr partial = *msg;
            struct iovec iov[2]{};
            size_t left = static_cast<size_t>(short_n);
            size_t n = 0;
            for (; n < msg->msg_iovlen && left > 0; ++n) {
                iov[n].iov_base = msg->msg_iov[n].iov_base;
                iov[n].iov_len = std::min(left, msg->msg_iov[n].iov_len);
                left -= iov[n].iov_len;
            }
            partial.msg_iov = iov;
            partial.msg_iovlen = n;
            return ::sendmsg(fd, &partial, flags);
        }
        return ::sendmsg(fd, msg, flags);
    }

    static int shutdown(int fd, int how) noexcept {
        shutdown_calls.fetch_add(1, std::memory_order_relaxed);
        const int e = shutdown_errno.exchange(0, std::memory_order_acq_rel);
        if (e != 0) {
            errno = e;
            return -1;
        }
        return ::shutdown(fd, how);
    }

    static int epoll_ctl(int epfd, int op, int fd,
                         struct epoll_event* ev) noexcept {
        epoll_ctl_calls.fetch_add(1, std::memory_order_relaxed);
        const int wanted_op = epoll_ctl_op.load(std::memory_order_relaxed);
        if (wanted_op == op) {
            const int e = epoll_ctl_errno.exchange(
                0, std::memory_order_acq_rel);
            if (e != 0) {
                epoll_ctl_op.store(-1, std::memory_order_release);
                errno = e;
                return -1;
            }
        }
        return ::epoll_ctl(epfd, op, fd, ev);
    }

    static int accept4(int fd, struct sockaddr* addr, socklen_t* len,
                       int flags) noexcept {
        accept4_calls.fetch_add(1, std::memory_order_relaxed);
        const int e = accept4_errno.exchange(0, std::memory_order_acq_rel);
        if (e != 0) {
            errno = e;
            return -1;
        }
        return ::accept4(fd, addr, len, flags);
    }
};

inline std::atomic<int> seam_policy::recv_errno{0};
inline std::atomic<int> seam_policy::sendmsg_errno{0};
inline std::atomic<int> seam_policy::sendmsg_short_bytes{0};
inline std::atomic<int> seam_policy::shutdown_errno{0};
inline std::atomic<int> seam_policy::epoll_ctl_errno{0};
inline std::atomic<int> seam_policy::epoll_ctl_op{-1};
inline std::atomic<int> seam_policy::accept4_errno{0};
inline std::atomic<uint64_t> seam_policy::recv_calls{0};
inline std::atomic<uint64_t> seam_policy::sendmsg_calls{0};
inline std::atomic<uint64_t> seam_policy::shutdown_calls{0};
inline std::atomic<uint64_t> seam_policy::epoll_ctl_calls{0};
inline std::atomic<uint64_t> seam_policy::accept4_calls{0};

}  // namespace industrial
