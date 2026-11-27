#!/usr/bin/env bash
# Run one verify/ case under Linux and report its real exit status.
#
# This exists because of a trap that already cost a verdict in this project:
# handing a command to a shell inline (`bash -lc '...; echo $?'`) lets the outer
# shell expand $? first, so a binary compiled to return 1 reports 0 and a red
# run looks green. The status has to be observed by a script file that travelled
# to disk, which is what this is.
#
# Usage: bash verify/evidence/run_one.sh <source-under-verify/> [args...]
set -uo pipefail

src="$1"; shift
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

# Same output directory as run.sh: under the home cache by default, never /tmp
# unless asked for. VERIFY_OUT_DIR overrides both.
out_dir="${VERIFY_OUT_DIR:-$HOME/.cache/tcp-server-verify}"
mkdir -p "$out_dir"
out="$out_dir/$(basename "$src" .cpp)"
# Suites include harness.h from verify/support (relocated from verify/ in the
# layering change), so both roots are needed -- -Iinclude alone cannot build
# any relocated suite.
flags=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -O1
       -Iinclude -Iverify/support)

# Two suites select their build at the source level, exactly as run.sh does:
# the FSM test includes the working header and the branch-point oracle under
# two macros, and the seam suite picks its policy in this translation unit.
case "$src" in
    *tcp_server_fsm_test.cpp)
        flags+=(-DNEWH='"tcp-server-mt.h"' -DOLDH='"tcp-server-mt.prev.h"')
        ;;
    *syscall_seam.cpp)
        flags+=(-DTCP_SERVER_SYSCALL_POLICY=industrial::seam_policy)
        ;;
esac

g++ "${flags[@]}" "$src" -o "$out" || { echo "build failed"; exit 2; }
"$out" "$@"
rc=$?
echo "rc=$rc"
exit "$rc"
