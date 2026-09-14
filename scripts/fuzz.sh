#!/usr/bin/env bash
# Runs Go native fuzz testing targets across TaskMQ.
# Usage:
#   ./scripts/fuzz.sh         # Runs standard seed corpus verification
#   ./scripts/fuzz.sh 5s      # Runs 5 seconds of mutation per target
#   ./scripts/fuzz.sh 30s     # Deep security audit run
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FUZZ_TIME="${1:-0}"

TARGETS=(
  "github.com/twn39/taskmq/internal/taskmq/codec FuzzBinaryCodec_Unmarshal"
  "github.com/twn39/taskmq/internal/taskmq/codec FuzzBinaryCodec_RoundTrip"
  "github.com/twn39/taskmq/internal/taskmq/codec FuzzInspectJSONField"
  "github.com/twn39/taskmq/internal/taskmq/codec FuzzUnsafeStringToBytes"
  "github.com/twn39/taskmq/internal/taskmq/keys FuzzParseQueueFromStreamKey"
  "github.com/twn39/taskmq/internal/taskmq/keys FuzzKeysFor_ClusterHashTags"
  "github.com/twn39/taskmq/internal/taskmq/runner FuzzCronParser"
)

echo "=== TaskMQ Go Native Fuzzing Suite ==="
if [[ "$FUZZ_TIME" == "0" ]]; then
  echo "Mode: Seed corpus verification only"
  for t in "${TARGETS[@]}"; do
    pkg="${t%% *}"
    target="${t##* }"
    echo "--> Verifying seeds for $target in $pkg"
    go test -v -run="^${target}$" "$pkg"
  done
else
  echo "Mode: Mutation fuzzing (duration: $FUZZ_TIME per target)"
  for t in "${TARGETS[@]}"; do
    pkg="${t%% *}"
    target="${t##* }"
    echo "--> Fuzzing $target in $pkg ($FUZZ_TIME)..."
    go test -fuzz="^${target}$" -fuzztime="$FUZZ_TIME" "$pkg"
  done
fi

echo "=== All Fuzz Targets Completed Successfully ==="
