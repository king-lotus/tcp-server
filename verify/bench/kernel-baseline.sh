#!/usr/bin/env bash
# Capture one kernel's benchmark baseline, so the RT and non-RT runs are the
# same measurement rather than two differently-parameterised ones.
#
#   bash verify/bench/kernel-baseline.sh
#   REPS=2 SECS=2 bash verify/bench/kernel-baseline.sh   # quicker
#
# Output: $VERIFY_OUT_DIR/kernel-$(uname -r)/
#   meta.txt            kernel, compiler, core count
#   run.log             bench/run.sh (RSS, ingest, send medians)
#   matrix.log          bench/matrix.sh (wbuf x client-placement)
#   perf-all.txt        every task in the fork-mode recording
#   perf-server.txt     only the reactor threads (server-side profile)
#   perf-fork.data      the raw recording
#
# Reboot into the other kernel and run the same script; the two directories
# are then comparable. perf is skipped with a note when perf_event_paranoid
# forbids it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

out="${VERIFY_OUT_DIR:-$HOME/.cache/tcp-server-verify}"
REPS="${REPS:-3}"
SECS="${SECS:-3}"
dest="$out/kernel-$(uname -r)"
mkdir -p "$dest"

{
    echo "kernel: $(uname -r)"
    uname -a
    g++ --version | head -1
    echo "cores: $(nproc)"
} | tee "$dest/meta.txt"

echo "== benchmarks =="
REPS="$REPS" SECS="$SECS" bash verify/bench/run.sh | tee "$dest/run.log"

echo "== matrix =="
REPS="$REPS" SECS="$SECS" bash verify/bench/matrix.sh | tee "$dest/matrix.log"

paranoid="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)"
if ! command -v perf >/dev/null 2>&1 ||
   { [ "$paranoid" -gt 1 ] && [ "$(id -u)" -ne 0 ]; }; then
    echo "== perf skipped (perf_event_paranoid=$paranoid) ==" | tee "$dest/perf.txt"
    exit 0
fi

echo "== perf: fork-mode recording =="
PERF_REACTORS="${PERF_REACTORS:-4}"
PERF_CLIENTS="${PERF_CLIENTS:-4}"
PERF_SECS="${PERF_SECS:-5}"
perf record -g -o "$dest/perf-fork.data" -- \
    env BENCH_CLIENT_FORK=1 "$out/bench_rx" \
    "$PERF_REACTORS" "$PERF_CLIENTS" "$PERF_SECS" >/dev/null 2>&1

perf report -i "$dest/perf-fork.data" --stdio --sort dso,symbol \
    --no-children --percent-limit 0.3 > "$dest/perf-all.txt" 2>/dev/null

# The reactor TIDs are the threads that execute parse_frames. Filtering by
# them drops the forked client process and answers "what does the server
# spend its own CPU on", which the combined profile cannot.
# `pid` is the sort key (there is no `tid` key); only the flat overhead lines
# have a Pid:Command second field, so the call-graph continuation lines that
# also mention parse_frames cannot contribute a bogus token.
tids=$(perf report -i "$dest/perf-fork.data" --stdio --sort pid,symbol \
           --no-children --percent-limit 0.1 2>/dev/null |
       awk '$1 ~ /%$/ && $2 ~ /:/ && /parse_frames/ {
                split($2, a, ":"); print a[1] }' |
       sort -u | paste -sd, -)
if [ -n "$tids" ]; then
    perf report -i "$dest/perf-fork.data" --tid="$tids" --stdio \
        --sort dso,symbol --no-children --percent-limit 0.1 \
        > "$dest/perf-server.txt" 2>/dev/null
    echo "   server TIDs: $tids" | tee -a "$dest/perf-server.txt"
else
    echo "   no parse_frames samples found; server-only report skipped" |
        tee "$dest/perf-server.txt"
fi

echo "baseline written to $dest"
