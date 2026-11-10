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
# The defaults are ABSOLUTE numbers measured on one development machine (an Apple
# Silicon laptop; its 24 MHz counter gives the ~41.7ns granularity above). On a
# different machine class (a Linux host with a ~1ns clock, a slower or throttled
# CPU) they mean something else: record that host's own limits instead. Order of
# precedence, highest first: the GET_1T_MAX_NS / GET_8T_MAX_NS environment
# variables, a per-host baseline file (shell assignments of those two variables;
# default scripts/get-regression.local, which .gitignore already excludes, or the
# path in GET_BASELINE_FILE), then the defaults.
#
# Tail latency on a shared machine is noisy: up to ATTEMPTS (default 3) runs
# are made and the guard passes if any one attempt is within both limits. The
# best-of-N policy can hide a regression that is only intermittently slow; the
# output names the attempt that passed so a host that needs retries is visible.
#
# `nomatch_filter` matches no Criterion benchmark, so only the explicit tail
# report runs (about 2 seconds after the build).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2

env_1t="${GET_1T_MAX_NS:-}"
env_8t="${GET_8T_MAX_NS:-}"
baseline_file="${GET_BASELINE_FILE:-scripts/get-regression.local}"
if [[ -f "$baseline_file" ]]; then
  # shellcheck disable=SC1090
  . "$baseline_file"
  echo "using per-host limits from $baseline_file"
fi
GET_1T_MAX_NS="${env_1t:-${GET_1T_MAX_NS:-334}}"
GET_8T_MAX_NS="${env_8t:-${GET_8T_MAX_NS:-667}}"
ATTEMPTS="${ATTEMPTS:-3}"

extract_p99() {
  # $1 = report text, $2 = label prefix, e.g. "GET 1 thread"
  printf '%s\n' "$1" \
    | grep -F "[tail] $2" \
    | head -n1 \
    | sed -n 's/.*p99= *\([0-9][0-9]*\)ns.*/\1/p' \
    || true  # no matching line is an empty result for the caller's diagnostic, not a set -e exit
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
    echo "PASS: GET tail latency within limits (attempt $attempt of $ATTEMPTS)"
    exit 0
  fi
done

echo "FAIL: GET p99 exceeded a limit on all $ATTEMPTS attempts (1 thread max ${GET_1T_MAX_NS}ns, 8 threads max ${GET_8T_MAX_NS}ns)" >&2
exit 1
