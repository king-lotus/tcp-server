#!/usr/bin/env bash
# Profile the ingest benchmark with Linux perf.
#
#   bash verify/bench/perf.sh              # 1 reactor, 1 client, 5 seconds
#   bash verify/bench/perf.sh 4 4 5        # 4 reactors, 4 clients
#
# The other bench scripts answer "how fast"; this one answers "where". It
# records a call graph and prints the symbol table with percentages. Build the
# benchmark with -g -fno-omit-frame-pointer so the samples resolve to functions
# rather than to libc offsets.
#
# perf is usually blocked for unprivileged users on hardened kernels:
# /proc/sys/kernel/perf_event_paranoid > 1 denies perf_event_open, and perf's
# only symptom is "Access to performance monitoring ... is limited". Running
# this script under sudo is the least invasive fix; lowering the sysctl opens
# profiling to every process on the machine.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

out="${VERIFY_OUT_DIR:-$HOME/.cache/tcp-server-verify}"
mkdir -p "$out"

perf_bin="${PERF:-perf}"
if ! command -v "$perf_bin" >/dev/null 2>&1; then
    echo "perf: not installed (apt install linux-tools-$(uname -r) or linux-tools-generic)" >&2
    exit 2
fi

paranoid="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)"
if [ "$paranoid" != "unknown" ] && [ "$paranoid" -gt 1 ] && [ "$(id -u)" -ne 0 ]; then
    cat >&2 <<EOF
perf: perf_event_open is restricted (perf_event_paranoid=$paranoid).
Run this script as root, which needs no sysctl change:

    sudo bash "$0" ${1:-1} ${2:-1} ${3:-5}

or temporarily allow profiling for your user:

    sudo sysctl kernel.perf_event_paranoid=1
EOF
    exit 2
fi

flags=(-std=c++17 -O2 -g -fno-omit-frame-pointer -pthread -Iinclude)
echo "== building [perf] =="
g++ "${flags[@]}" verify/bench/rx.cpp -o "$out/bench_rx_perf"

echo "== recording =="
"$perf_bin" record -F 999 -g --call-graph dwarf \
    -o "$out/perf.data" -- "$out/bench_rx_perf" \
    "${1:-1}" "${2:-1}" "${3:-5}"

echo "== report (samples by symbol) =="
"$perf_bin" report -i "$out/perf.data" --stdio \
    --sort comm,dso,symbol --percent-limit 0.5
