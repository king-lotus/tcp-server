#!/usr/bin/env bash
# R5 claims the internal types are no longer reachable as industrial::Conn,
# industrial::SubReactor and so on. That is a claim about what does NOT
# compile, and a claim about a compile failing needs a control -- otherwise
# "it failed" and "the compiler never ran" look exactly the same. This project
# has already paid for that lesson twice: the cpplint check carries a control
# file, and harness_selftest's RST case is still red on a kernel that cannot
# produce the thing it asserts.
#
# So: one translation unit that must compile, one that must not.
#
#     bash verify/evidence/probe_surface_shrank.sh
#
# Exit 0 means the surface is what R5 says it is. Where the shell in front of
# this is a wrapper whose exit status cannot be trusted -- a nested bash under
# wsl.exe does exactly that here, and so does verify/run.sh -- read the PASS or
# FAIL line instead. That line is the verdict; the status is a convenience for
# CI.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

cxx="${CXX:-g++}"
flags=(-std=c++17 -fsyntax-only -Iinclude)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# ---- control: a public type has to be reachable, or nothing below means
# anything -------------------------------------------------------------------
cat > "$tmp/control.cpp" <<'CPP'
#include "tcp-server-mt.h"
int main() {
    industrial::MultiReactorServer::Config cfg;
    (void)cfg;
    return 0;
}
CPP

if ! "$cxx" "${flags[@]}" "$tmp/control.cpp" 2>"$tmp/control.err"; then
    echo "[FAIL] the control did not compile: the header itself is broken"
    tail -5 "$tmp/control.err"
    exit 1
fi
echo "  control: industrial::MultiReactorServer reached, as expected"

# ---- probe: the names R5 moved into detail must be gone from industrial:: ---
fail=0
for name in Conn SubReactor FdGuard RingBuffer Admission ClosedConn \
            DrainOutcome CloseState ClosePhase CloseEvent CloseAction \
            CloseFacts CloseRound; do
    cat > "$tmp/probe.cpp" <<CPP
#include "tcp-server-mt.h"
int main() {
    industrial::$name* p = nullptr;
    (void)p;
    return 0;
}
CPP
    if "$cxx" "${flags[@]}" "$tmp/probe.cpp" 2>/dev/null; then
        echo "  [FAIL] industrial::$name is still reachable"
        fail=1
    fi
done

# ---- and the one internal function that deliberately stayed public ---------
cat > "$tmp/enc.cpp" <<'CPP'
#include "tcp-server-mt.h"
int main() {
    uint8_t buf[16];
    industrial::encodeFrameInto(buf, industrial::FrameType::DATA, "x", 1);
    return 0;
}
CPP
if ! "$cxx" "${flags[@]}" "$tmp/enc.cpp" 2>/dev/null; then
    echo "  [FAIL] industrial::encodeFrameInto is no longer reachable"
    echo "         (every suite builds frames with it; it is protocol surface)"
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "  PASS: 13 internal names gone, 1 public encoder kept"
else
    echo "  FAIL: the public surface is wider than R5 claims"
fi
# Explicit, not `[ ... ] && echo` on the last line: under set -e that form
# decides the exit status by accident, and a probe that reports FAIL while
# exiting 0 is the exact failure mode this file exists to avoid.
exit "$fail"
