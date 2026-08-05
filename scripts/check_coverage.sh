#!/usr/bin/env bash
# Enforce minimum unit-test coverage from coverage.yaml.
#
# Usage:
#   ./scripts/check_coverage.sh              # gates only
#   ./scripts/check_coverage.sh --report     # gates + merged profile/HTML
#   CONFIG=coverage.yaml ./scripts/check_coverage.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONFIG="${CONFIG:-coverage.yaml}"
WANT_REPORT=0
for arg in "$@"; do
  case "$arg" in
    --report|-r) WANT_REPORT=1 ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/check_coverage.sh [--report]

  --report   Also write coverage.out + coverage.html (paths from coverage.yaml)

Config: coverage.yaml (override with CONFIG=path)
EOF
      exit 0
      ;;
  esac
done

if [[ ! -f "$CONFIG" ]]; then
  echo "coverage config not found: $CONFIG" >&2
  exit 2
fi

TMPDIR_COV="$(mktemp -d -t taskmq-cover.XXXXXX)"
trap 'rm -rf "$TMPDIR_COV"' EXIT

# Parse coverage.yaml into flat files (avoid multiline shell exports).
python3 - "$CONFIG" "$TMPDIR_COV" <<'PY'
import re, sys
from pathlib import Path

cfg_path, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
text = cfg_path.read_text()
lines = []
for line in text.splitlines():
    if "#" in line:
        line = line[: line.index("#")]
    lines.append(line.rstrip())
text = "\n".join(lines)

def scalar(key, default=""):
    m = re.search(rf"(?m)^{re.escape(key)}:\s*(.+)\s*$", text)
    if not m:
        return default
    return m.group(1).strip().strip('"').strip("'")

def list_block(key):
    # Do not use re.S: '.' must not cross newlines or list sections bleed together.
    m = re.search(
        rf"(?m)^{re.escape(key)}:\s*\n((?:[ \t]+-[ \t]+[^\n]+\n?)+)",
        text,
    )
    if not m:
        return []
    out = []
    for line in m.group(1).splitlines():
        mm = re.match(r"[ \t]+-[ \t]+(.+)$", line)
        if mm:
            out.append(mm.group(1).strip().strip('"').strip("'"))
    return out

def packages_block():
    # Capture from "packages:" until next top-level key or EOF; blank lines allowed.
    m = re.search(r"(?m)^packages:\s*\n(.*?)(?=^[a-zA-Z_][\w-]*:|\Z)", text, re.S)
    if not m:
        return []
    out = []
    for line in m.group(1).splitlines():
        mm = re.match(r"[ \t]+(\S+):\s*(\d+)\s*$", line)
        if mm:
            out.append((mm.group(1), int(mm.group(2))))
    return out

mode = scalar("mode", "atomic")
timeout = scalar("timeout", "5m")
profile = scalar("profile", "coverage.out")
html = scalar("html", "coverage.html")
overall = scalar("overall_min", "0")
report = list_block("report") or ["./internal/...", "./tests/unit/..."]
exclude = list_block("exclude")
packages = packages_block()

(out_dir / "mode").write_text(mode)
(out_dir / "timeout").write_text(timeout)
(out_dir / "profile").write_text(profile)
(out_dir / "html").write_text(html)
(out_dir / "overall_min").write_text(overall)
(out_dir / "packages.tsv").write_text("".join(f"{p}\t{n}\n" for p, n in packages))
(out_dir / "report.txt").write_text("".join(f"{p}\n" for p in report))
(out_dir / "exclude.txt").write_text("".join(f"{e}\n" for e in exclude))
PY

COVER_MODE="$(cat "$TMPDIR_COV/mode")"
COVER_TIMEOUT="$(cat "$TMPDIR_COV/timeout")"
COVER_PROFILE="$(cat "$TMPDIR_COV/profile")"
COVER_HTML="$(cat "$TMPDIR_COV/html")"
COVER_OVERALL_MIN="$(cat "$TMPDIR_COV/overall_min")"

if [[ ! -s "$TMPDIR_COV/packages.tsv" ]]; then
  echo "no packages defined in $CONFIG" >&2
  exit 2
fi

fail=0
echo "==> Coverage gate (config: $CONFIG, mode=$COVER_MODE)"
printf "%-70s %8s %8s %s\n" "PACKAGE" "HAVE" "NEED" "STATUS"
printf "%-70s %8s %8s %s\n" "-------" "----" "----" "------"

while IFS=$'\t' read -r pkg need; do
  [[ -z "${pkg:-}" ]] && continue
  out="$(go test "$pkg" -cover -covermode="$COVER_MODE" -count=1 -timeout "$COVER_TIMEOUT" 2>&1)" || true
  pct="$(echo "$out" | sed -n 's/.*coverage: \([0-9.]*\)% of statements.*/\1/p' | tail -1)"
  if [[ -z "$pct" ]]; then
    if echo "$out" | grep -q "no test files"; then
      pct="0.0"
    else
      echo "FAILED to measure coverage for $pkg"
      echo "$out" | tail -20
      fail=1
      continue
    fi
  fi
  have_i="${pct%.*}"
  if (( have_i >= need )); then
    status="OK"
  else
    status="FAIL"
    fail=1
  fi
  printf "%-70s %7s%% %7s%% %s\n" "$pkg" "$pct" "$need" "$status"
done <"$TMPDIR_COV/packages.tsv"

echo ""

if (( WANT_REPORT == 1 )); then
  echo "==> Writing merged profile → $COVER_PROFILE"
  cleaned=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "${line// }" ]] && continue
    cleaned+=("$line")
  done <"$TMPDIR_COV/report.txt"
  if [[ ${#cleaned[@]} -eq 0 ]]; then
    cleaned=(./internal/... ./tests/unit/...)
  fi

  go test "${cleaned[@]}" \
    -covermode="$COVER_MODE" \
    -coverprofile="$COVER_PROFILE.raw" \
    -count=1 \
    -timeout 10m \
    >/dev/null

  python3 - "$COVER_PROFILE.raw" "$COVER_PROFILE" "$TMPDIR_COV/exclude.txt" <<'PY'
import sys
from pathlib import Path

raw_path, out_path, exclude_path = sys.argv[1], sys.argv[2], sys.argv[3]
excludes = [e for e in Path(exclude_path).read_text().splitlines() if e.strip()]
lines = Path(raw_path).read_text().splitlines()
if not lines:
    Path(out_path).write_text("")
    raise SystemExit(0)
out = [lines[0]]
for line in lines[1:]:
    path = line.split(":", 1)[0]
    if any(ex in path for ex in excludes):
        continue
    out.append(line)
Path(out_path).write_text("\n".join(out) + ("\n" if out else ""))
PY
  rm -f "$COVER_PROFILE.raw"

  total_line="$(go tool cover -func="$COVER_PROFILE" | tail -n 1)"
  echo "    $total_line"
  total_pct="$(echo "$total_line" | awk '{print $NF}' | tr -d '%')"
  if [[ -n "$total_pct" && "$COVER_OVERALL_MIN" != "0" ]]; then
    total_i="${total_pct%.*}"
    if (( total_i < COVER_OVERALL_MIN )); then
      echo "overall coverage ${total_pct}% < overall_min ${COVER_OVERALL_MIN}%"
      fail=1
    else
      echo "overall coverage ${total_pct}% (min ${COVER_OVERALL_MIN}%) OK"
    fi
  fi

  go tool cover -html="$COVER_PROFILE" -o "$COVER_HTML"
  echo "    HTML report → $COVER_HTML"
  echo ""
fi

if (( fail != 0 )); then
  echo "Coverage gate FAILED (edit thresholds in $CONFIG only with review)"
  exit 1
fi
echo "Coverage gate passed"
