#!/usr/bin/env bash
# Guardrail: Redis key schema lives only in internal/taskmq/keys via KeysFor/QueueKeys.
# Run from repo root: ./scripts/check_keys_schema.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "check_keys_schema: ripgrep (rg) is required" >&2
  exit 2
fi

# 1) taskmq:{ literals only allowed in the keys package.
matches="$(rg -n 'taskmq:\{' \
  --glob '*.go' \
  --glob '!internal/taskmq/keys/**' \
  --glob '!third_party/**' \
  || true)"

if [[ -n "$matches" ]]; then
  echo "ERROR: taskmq:{ key literals found outside internal/taskmq/keys:" >&2
  echo "$matches" >&2
  echo "" >&2
  echo "Use keys.KeysFor(queue).Method() instead of fmt.Sprintf / hardcoded strings." >&2
  exit 1
fi

# 2) Removed free key helpers must not reappear as package-level funcs or package-qualified calls.
#    (QueueKeys methods use different names: Stream/Delayed/DLQ/… not StreamKey/DelayedKey/…)
removed="$(rg -n \
  -e '^func (StreamKey|DelayedKey|DLQKey|DLQIndexKey|UniqueKey|CronConfigsKey|PausedKey|ControlChannel|CancelledKey|CancelChannel|DelayedWakeupChannel)\(' \
  -e '\b(keys|mqkeys)\.(StreamKey|DelayedKey|DLQKey|DLQIndexKey|UniqueKey|CronConfigsKey|PausedKey|ControlChannel|CancelledKey|CancelChannel|DelayedWakeupChannel)\(' \
  --glob '*.go' \
  --glob '!third_party/**' \
  || true)"

if [[ -n "$removed" ]]; then
  echo "ERROR: removed free key helpers reintroduced:" >&2
  echo "$removed" >&2
  echo "" >&2
  echo "Use keys.KeysFor(queue).Method() only." >&2
  exit 1
fi

echo "check_keys_schema: OK"
