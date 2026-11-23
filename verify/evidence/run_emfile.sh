#!/usr/bin/env bash
# Build and run the EMFILE probe with assertions. Linux only.
#
#   bash verify/evidence/run_emfile.sh
#
# Kept outside verify/run.sh's three-configuration matrix on purpose: this is
# a kernel-behaviour probe, not a memory- or race-sanitizer claim. CI calls it
# in a separate step; locally ENABLE_SLOW=1 bash verify/run.sh calls the same
# script after the ordinary suites. A host that never reaches EMFILE prints
# [SKIP] and does not fail the job -- the descriptor ceiling is lowered by the
# probe itself, but what the accept queue does at that ceiling is up to the
# kernel (WSL 1 dequeues and closes instead of leaving the connection queued).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

out="${VERIFY_OUT_DIR:-$HOME/.cache/tcp-server-verify}"
mkdir -p "$out"

flags=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -Iinclude)
echo "== building [emfile probe] =="
g++ "${flags[@]}" verify/evidence/probe_emfile.cpp -o "$out/probe_emfile"
echo "---- running [emfile probe] ----"
timeout 180 "$out/probe_emfile"
