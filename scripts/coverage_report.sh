#!/usr/bin/env bash
# Generate merged coverage profile + HTML from coverage.yaml (no gate).
# Usage: ./scripts/coverage_report.sh
#        open coverage.html
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec ./scripts/check_coverage.sh --report
