#!/usr/bin/env bash
# Cross the two knobs that decide what the ingest number actually means.
#
#   bash verify/bench_matrix.sh
#   REPS=1 SECS=2 bash verify/bench_matrix.sh     # quicker, noisier
#
# Row dimension 1: the per-connection write buffer (BENCH_WBUF_KB). It is what
# per-connection RSS is made of, and it also caps what a reactor can queue, so
# shrinking it trades memory for the size of a reply burst.
#
# Row dimension 2: where the client lives (BENCH_CLIENT_FORK). In the same
# process the generator steals the cores the server would use, so the number is
# a floor for both halves. In a child process the server gets the machine, and
# "why did 4 reactors not beat 1" finally has an answer either way.
#
# Every cell is the median of REPS runs. Nothing here is a pass/fail -- these
# are numbers to compare against numbers taken on the same machine.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$here")"
cd "$proj"

REPS="${REPS:-3}"
SECS="${SECS:-3}"
flags=(-std=c++17 -O2 -pthread -Iinclude)

echo "== building =="
g++ "${flags[@]}" verify/bench_rss.cpp  -o /tmp/m_rss
g++ "${flags[@]}" verify/bench_rx.cpp   -o /tmp/m_rx
g++ "${flags[@]}" verify/bench_send.cpp -o /tmp/m_send
g++ --version | head -1
echo "   cores: $(nproc)   affinity: $(taskset -cp $$ 2>/dev/null | sed 's/.*list: //')"

field() { sed -n "s/.* $1=\([0-9.][0-9.]*\).*/\1/p"; }
median() { sort -n | awk '{a[NR]=$1} END{ if (NR==0) {print "nan"; exit} if (NR%2) print a[(NR+1)/2]; else print (a[NR/2]+a[NR/2+1])/2 }'; }

# row <wbuf|default> <same|fork> -> fills RSS1 R1 R2 R4 P50 P99
row() {
    local wbuf="$1" mode="$2"
    local envs=()
    [ "$wbuf" != "default" ] && envs+=("BENCH_WBUF_KB=$wbuf")
    [ "$mode" = "fork" ] && envs+=("BENCH_CLIENT_FORK=1")

    local tmp; tmp=$(mktemp)
    for _ in $(seq "$REPS"); do env "${envs[@]}" /tmp/m_rss 1000  >>"$tmp"; done
    RSS1=$(grep '^rss' "$tmp" | field per_conn_kb | median)

    : >"$tmp"
    for _ in $(seq "$REPS"); do env "${envs[@]}" /tmp/m_rx 1 1 "$SECS" >>"$tmp"; done
    R1=$(grep '^ingest' "$tmp" | field kframes_per_s | median)

    : >"$tmp"
    for _ in $(seq "$REPS"); do env "${envs[@]}" /tmp/m_rx 2 2 "$SECS" >>"$tmp"; done
    R2=$(grep '^ingest' "$tmp" | field kframes_per_s | median)

    : >"$tmp"
    for _ in $(seq "$REPS"); do env "${envs[@]}" /tmp/m_rx 4 4 "$SECS" >>"$tmp"; done
    R4=$(grep '^ingest' "$tmp" | field kframes_per_s | median)

    : >"$tmp"
    for _ in $(seq "$REPS"); do env "${envs[@]}" /tmp/m_send 2000 56 >>"$tmp"; done
    P50=$(grep '^send' "$tmp" | field p50_us | median)
    P99=$(grep '^send' "$tmp" | field p99_us | median)

    rm -f "$tmp"
}

printf '\n%-8s %-6s %10s %10s %10s %10s %8s %8s\n' \
       "wbuf" "mode" "rss/conn" "ing1" "ing2" "ing4" "p50us" "p99us"
printf '%s\n' "-----------------------------------------------------------------------------"

for spec in "default same" "128 same" "64 same" "32 same" "default fork" "64 fork"; do
    set -- $spec
    echo "   ... running wbuf=$1 mode=$2" >&2
    row "$1" "$2"
    printf '%-8s %-6s %10.1f %10.0f %10.0f %10.0f %8.1f %8.1f\n' \
           "$1" "$2" "$RSS1" "$R1" "$R2" "$R4" "$P50" "$P99"
done

echo
echo "rss/conn in KB, ing* in kframes/s (median of $REPS runs, $SECS s each)"
