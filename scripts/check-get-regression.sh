#!/usr/bin/env bash
# Phase 3 no-regression gate for the stamped KvIndex cell.
#
# Runs the Phase 2 tail-latency report (benches/local_engine.rs) and checks the
# GET p99 values against thresholds derived from the Phase 2 baselines:
#   baseline at Phase 2 completion: GET 1 thread p99 = 250ns,
#                                   GET 8 threads p99 = 500-542ns
#   timer granularity is ~41.7ns, so the limits are about +25%:
#   GET_1T_MAX_NS (default 334), GET_8T_MAX_NS (default 667).
#
# Tail latency on a shared machine is noisy: up to ATTEMPTS (default 3) runs
# are made and the guard passes if any one attempt is within both limits.
#
# `nomatch_filter` matches no Criterion benchmark, so only the explicit tail
# report runs (about 2 seconds after the build).
set -euo pipefail

GET_1T_MAX_NS="${GET_1T_MAX_NS:-334}"
GET_8T_MAX_NS="${GET_8T_MAX_NS:-667}"
ATTEMPTS="${ATTEMPTS:-3}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

extract_p99() {
  # $1 = report text, $2 = label prefix, e.g. "GET 1 thread"
  printf '%s\n' "$1" \
    | grep -F "[tail] $2" \
    | head -n1 \
    | sed -n 's/.*p99= *\([0-9][0-9]*\)ns.*/\1/p'
}

for attempt in $(seq 1 "$ATTEMPTS"); do
  out="$(cargo bench --bench local_engine -- nomatch_filter 2>&1)" || {
    printf '%s\n' "$out" >&2
    echo "FAIL: cargo bench failed" >&2
    exit 1
  }
  p99_1t="$(extract_p99 "$out" "GET 1 thread")"
  p99_8t="$(extract_p99 "$out" "GET 8 threads")"
  if [[ -z "$p99_1t" || -z "$p99_8t" ]]; then
    printf '%s\n' "$out" >&2
    echo "FAIL: could not parse GET p99 values from the tail report" >&2
    exit 1
  fi
  echo "attempt $attempt: GET 1 thread p99=${p99_1t}ns (max ${GET_1T_MAX_NS}), GET 8 threads p99=${p99_8t}ns (max ${GET_8T_MAX_NS})"
  if (( p99_1t <= GET_1T_MAX_NS && p99_8t <= GET_8T_MAX_NS )); then
    echo "PASS: GET tail latency within limits"
    exit 0
  fi
done

echo "FAIL: GET p99 exceeded a limit on all $ATTEMPTS attempts (1 thread max ${GET_1T_MAX_NS}ns, 8 threads max ${GET_8T_MAX_NS}ns)" >&2
exit 1
