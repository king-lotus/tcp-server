#!/usr/bin/env bash
# probe_test_hooks_private.sh -- the fault switches are private, not public.
#
# What this settles: `fail_worker_reactor_for_test_` and its siblings are
# always-present members (a class definition that varies with a macro is an ODR
# hazard), and the claim under test is that being present does not mean being
# reachable. Two compilations:
#
#   1. any code holding a server reference that touches a switch directly must
#      be refused by the compiler, with "private" in the diagnostic -- not by a
#      comment asking people not to;
#   2. the friend accessor in verify/harness.h must reach them, or the
#      fault-injection suite stops compiling and the switches lose their only
#      consumer.
#
# Both halves matter. A probe that only proved (2) would pass just as well if
# the members were public again, which is the state this exists to prevent.
#
# Linux only. Exit code 0 = both halves behaved as claimed.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/direct.cpp" <<'EOF'
#include "tcp-server-mt.h"
void fire(industrial::multi_reactor_server& s) {
    s.fail_worker_reactor_for_test_.store(0);
}
EOF

cat > "$tmp/via_friend.cpp" <<'EOF'
#include "harness.h"
void fire(industrial::multi_reactor_server& s) {
    industrial::test_access::fail_worker_reactor_once(s, 0);
}
EOF

flags=(-std=c++17 -Iinclude -Iverify -fsyntax-only)

if g++ "${flags[@]}" "$tmp/direct.cpp" 2> "$tmp/direct.log"; then
    echo "  [FAIL] a switch is reachable from arbitrary code: the direct write compiled"
    exit 1
fi
if ! grep -qi "private" "$tmp/direct.log"; then
    echo "  [FAIL] the direct write failed, but not because the member is private:"
    sed -n '1,5p' "$tmp/direct.log"
    exit 1
fi
echo "  [PASS] a direct write is refused: $(grep -m1 -o 'is private' "$tmp/direct.log")"

if ! g++ "${flags[@]}" "$tmp/via_friend.cpp" 2> "$tmp/friend.log"; then
    echo "  [FAIL] the friend accessor cannot reach the switches:"
    sed -n '1,10p' "$tmp/friend.log"
    exit 1
fi
echo "  [PASS] industrial::test_access still reaches them"

echo "settled: present in every build, reachable only through the friend accessor"
