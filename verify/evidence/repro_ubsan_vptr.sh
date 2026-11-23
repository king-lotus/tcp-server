#!/bin/bash
# Reproduce the ASan+UBSan vptr report in teardown_closed().
#
#   /usr/include/c++/11/bits/shared_ptr_base.h:705: runtime error: member call on
#   address 0x508000000020 which does not point to an object of type
#   '_Sp_counted_base'
#
# Stack: finish_peer_closed -> teardown_closed, always the same frame.
#
# What makes it appear: ASan AND UBSan's vptr check together, plus concurrency.
#   - UBSan alone:      0 hits in 10 sequential runs
#   - ASan, vptr off:   0 hits in 16 concurrent runs
#   - ASan + UBSan:     4 of 4 workers hit within 12 iterations each
# Sequential runs of the same build hit roughly one time in three; running
# several instances at once is what makes it reliable.
#
# Usage (from the repo root on Linux):
#   bash verify/evidence/repro_ubsan_vptr.sh [workers] [iterations]
set -u

root=$(cd "$(dirname "$0")/../.." && pwd)
workers=${1:-4}
iters=${2:-12}

g++ -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    -I"$root/include" "$root/verify/evidence/probe_emfile.cpp" \
    -o /tmp/probe_emfile_asan -pthread || exit 1

rm -f /tmp/ubsan_hit_*.txt

worker() {
  local id=$1
  local port=$((39000 + id * 7))
  local i
  for i in $(seq 1 "$iters"); do
    out=$(UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
          ASAN_OPTIONS=detect_leaks=0 \
          timeout 120 /tmp/probe_emfile_asan "$port" 2>&1)
    if printf '%s' "$out" | grep -q "runtime error"; then
      printf '%s\n' "$out" > "/tmp/ubsan_hit_$id.txt"
      echo "worker $id hit on iteration $i"
      return 0
    fi
  done
  echo "worker $id clean"
}

for w in $(seq 1 "$workers"); do worker "$w" & done
wait

if ls /tmp/ubsan_hit_*.txt >/dev/null 2>&1; then
  echo "=== report ==="
  grep -m1 -A8 "runtime error" "$(ls /tmp/ubsan_hit_*.txt | head -1)"
  exit 0
fi
echo "no report in $workers workers x $iters iterations"
exit 1
