#!/bin/bash
# Case F (a close racing the reply it produced) is a probabilistic detector:
# the window is microseconds wide and one run of the test tells you almost
# nothing. This drives N runs against two builds and counts how many of them
# reproduce, which is the only comparison worth reading.
#
#   before = the tree at a given revision (defaults to HEAD)
#   after  = the working tree
#
# Usage:
#   bash verify/evidence/loop_close_race.sh [N] [ref-for-before]
#
# Measured on this machine, 20 rounds per run, ThreadSanitizer, CS_REACTORS=1:
#   before the drainInputThenClose fix: 4 of 8 runs reproduced
#   after:                              0 of 8 runs reproduced
#
# A run that times out, fails to launch, or never prints the result line is
# counted as invalid, not as a reproduction. Any invalid run makes the script
# exit nonzero: a launch failure must not be read as evidence for the race.
set -u
PROJ=${PROJ:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
N=${1:-8}
BEFORE_REF=${2:-HEAD}
cd "$PROJ" || exit 9

# TSan reserves a fixed shadow region and cannot start when the kernel hands
# out high vm.mmap_rnd_bits (Ubuntu 24.04+). Without this, every run dies before
# main(), the result line is missing, and the script counts it as a
# reproduction -- the opposite of what happened.
launcher=()
if command -v setarch >/dev/null 2>&1; then
  launcher=(setarch "$(uname -m)" -R)
fi

rm -rf /tmp/cfr_before /tmp/cfr_after
mkdir -p /tmp/cfr_before/verify /tmp/cfr_after/verify

# The header moved to include/ on 2026-10-18. Revs from before that still carry
# it at the repository root, and `git show` to a path a rev does not have just
# fails -- so ask which exists rather than assuming. Same question twice, since
# a probe that works on half the history is no probe at all.
hdr_in() {
  for p in include/tcp-server-mt.h tcp-server-mt.h; do
    if git cat-file -e "$1:$p" 2>/dev/null; then printf '%s' "$p"; return 0; fi
  done
  echo "loop_close_race: no tcp-server-mt.h in $1" >&2
  return 1
}
HDR_BEFORE=$(hdr_in "$BEFORE_REF") || exit 9
HDR_AFTER=include/tcp-server-mt.h
[ -f "$HDR_AFTER" ] || HDR_AFTER=tcp-server-mt.h

# The suite moved to verify/suites/ and harness.h moved to verify/support/ in
# the layering change. Probe both layouts for the before side; the after side
# uses the relocated paths. Both files land at the throwaway tree's root so
# the bare #include "harness.h" / "tcp-server-mt.h" resolve with -I.
src_in() {
  for p in verify/suites/close_semantics.cpp verify/close_semantics.cpp; do
    if git cat-file -e "$1:$p" 2>/dev/null; then printf '%s' "$p"; return 0; fi
  done
  echo "loop_close_race: no close_semantics.cpp in $1" >&2
  return 1
}
harness_in() {
  for p in verify/support/harness.h verify/harness.h; do
    if git cat-file -e "$1:$p" 2>/dev/null; then printf '%s' "$p"; return 0; fi
  done
  echo "loop_close_race: no harness.h in $1" >&2
  return 1
}
SRC_BEFORE=$(src_in "$BEFORE_REF") || exit 9
HARNESS_BEFORE=$(harness_in "$BEFORE_REF") || exit 9

git show "$BEFORE_REF:$HDR_BEFORE" > /tmp/cfr_before/tcp-server-mt.h || exit 9
git show "$BEFORE_REF:$SRC_BEFORE" > /tmp/cfr_before/verify/close_semantics.cpp || exit 9
git show "$BEFORE_REF:$HARNESS_BEFORE" > /tmp/cfr_before/harness.h || exit 9
cp "$PROJ/$HDR_AFTER"                 /tmp/cfr_after/tcp-server-mt.h
cp "$PROJ/verify/suites/close_semantics.cpp" /tmp/cfr_after/verify/close_semantics.cpp
cp "$PROJ/verify/support/harness.h"  /tmp/cfr_after/harness.h

FLAGS=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -g
       -fsanitize=thread -fno-omit-frame-pointer)

bad=0
for label in before after; do
  build_args=("${FLAGS[@]}")
  src_dir=/tmp/cfr_$label
  # Deliberately unpatched: adding fprintf inside the close path changes the
  # timing of the very window being measured, which would make the two sides
  # incomparable. Tracing is a separate step -- see trace_close_race.py.
  # -I. matters now: close_semantics.cpp includes the header by bare name, and
  # the copy under test sits at the root of this throwaway tree.
  ( cd "$src_dir" && g++ "${build_args[@]}" -I. verify/close_semantics.cpp \
      -o /tmp/cfr_bin_$label ) || exit 9
  repro=0
  clean=0
  invalid=0
  for i in $(seq 1 "$N"); do
    rm -f /tmp/cfr_run.out
    CS_REACTORS=1 timeout 300 "${launcher[@]}" /tmp/cfr_bin_$label \
        > /tmp/cfr_run.out 2>/tmp/cfr_run.err
    rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "$label run $i rc=$rc :: INVALID (no verdict; timeout or launch failure)"
      invalid=$((invalid+1)); bad=1; continue
    fi
    line=$(grep -m1 "of 20 rounds lost" /tmp/cfr_run.out || true)
    echo "$label run $i rc=$rc :: ${line:-INVALID (no result line)}"
    if [ -z "$line" ]; then
      invalid=$((invalid+1)); bad=1; continue
    fi
    # Parse the count, not a substring: "10 of 20 rounds lost" contains
    # "0 of 20 rounds lost", so a pattern match would count a repro as clean.
    lost=$(printf '%s\n' "$line" |
           sed -n 's/^[[:space:]]*\([0-9][0-9]*\) of 20 rounds lost.*/\1/p')
    if [ -z "$lost" ]; then
      invalid=$((invalid+1)); bad=1
    elif [ "$lost" -eq 0 ]; then
      clean=$((clean+1))
    else
      repro=$((repro+1))
    fi
  done
  echo "== $label: clean=$clean repro=$repro invalid=$invalid (of $N)"
  echo "   last trace retained at /tmp/cfr_run.err"
done
[ "$bad" -eq 0 ] || {
  echo "one or more runs produced no verdict; the counts above are not a measurement" >&2
}
exit "$bad"
