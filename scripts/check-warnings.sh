#!/usr/bin/env bash
# Shared zero-warning gate for Phase 3.
#
# Runs `cargo check --all-targets` and fails on compile errors or on any diagnostic line that is
# not recorded verbatim in scripts/cargo-check-baseline.txt (third-party notices recorded right
# after the dependency add; expected empty).
set -u

cd "$(dirname "$0")/.."

baseline="scripts/cargo-check-baseline.txt"
output="$(cargo check --all-targets --quiet 2>&1)"
status=$?

if [ -s "$baseline" ]; then
  remaining="$(printf '%s\n' "$output" | grep -vxFf "$baseline" || true)"
else
  remaining="$output"
fi

# Ignore blank lines.
remaining="$(printf '%s\n' "$remaining" | grep -v '^[[:space:]]*$' || true)"

if [ -n "$remaining" ] || [ "$status" -ne 0 ]; then
  echo "new cargo diagnostics"
  printf '%s\n' "$remaining"
  exit 1
fi

exit 0
