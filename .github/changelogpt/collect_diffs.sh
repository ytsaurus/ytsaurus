#!/usr/bin/env bash
#
# collect_diffs.sh — Collect git diffs for changelog generation.
#
# Usage:
#   ./collect_diffs.sh \
#     --from-commit <old_commit_exclusive> \
#     --to-commit   <new_commit_inclusive> \
#     --use-path    <path> [--use-path <path2> ...]
#
# Example:
#   .github/changelogpt/collect_diffs.sh \
#     --from-commit 2788466412f56e941044e833dbfc201d1937807f \
#     --to-commit   db1ab343327831b0ea499e6e5c7a47db77fa08df \
#     --use-path    yt/python
#
# Output:
#   .github/changelogpt/task/<TIMESTAMP>/
#     ├── task_description.json
#     ├── 0001_<sha7>.diff
#     ├── 0002_<sha7>.diff
#     └── ...

set -euo pipefail

# ─── Defaults ────────────────────────────────────────────────────────────────
FROM_COMMIT=""
TO_COMMIT=""
USE_PATHS=()

# ─── Parse arguments ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from-commit)
            FROM_COMMIT="$2"
            shift 2
            ;;
        --to-commit)
            TO_COMMIT="$2"
            shift 2
            ;;
        --use-path)
            USE_PATHS+=("$2")
            shift 2
            ;;
        -h|--help)
            sed -n '2,/^$/s/^# \?//p' "$0"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# ─── Validate ────────────────────────────────────────────────────────────────
if [[ -z "$FROM_COMMIT" ]]; then
    echo "ERROR: --from-commit is required" >&2
    exit 1
fi
if [[ -z "$TO_COMMIT" ]]; then
    echo "ERROR: --to-commit is required" >&2
    exit 1
fi
if [[ ${#USE_PATHS[@]} -eq 0 ]]; then
    echo "ERROR: at least one --use-path is required" >&2
    exit 1
fi

# Resolve to the repo root so paths are always relative to it
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Verify commits exist
if ! git cat-file -e "${FROM_COMMIT}^{commit}" 2>/dev/null; then
    echo "ERROR: --from-commit ${FROM_COMMIT} not found in repository" >&2
    exit 1
fi
if ! git cat-file -e "${TO_COMMIT}^{commit}" 2>/dev/null; then
    echo "ERROR: --to-commit ${TO_COMMIT} not found in repository" >&2
    exit 1
fi

# ─── Prepare output directory ────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date '+%Y-%m-%d-%H-%M')"
OUTPUT_DIR="${SCRIPT_DIR}/task/${TIMESTAMP}"

mkdir -p "$OUTPUT_DIR"

echo "==> Output directory: ${OUTPUT_DIR}"

# ─── Collect commits ─────────────────────────────────────────────────────────
# git log FROM..TO means: commits reachable from TO but not from FROM.
# --reverse gives oldest-first order.
mapfile -t COMMITS < <(
    git log --reverse --format='%H' "${FROM_COMMIT}..${TO_COMMIT}" -- "${USE_PATHS[@]}"
)

TOTAL=${#COMMITS[@]}

if [[ "$TOTAL" -eq 0 ]]; then
    echo "WARNING: No commits found in range ${FROM_COMMIT}..${TO_COMMIT} for paths: ${USE_PATHS[*]}" >&2
    exit 0
fi

echo "==> Found ${TOTAL} commit(s) touching: ${USE_PATHS[*]}"

# ─── Save task_description.json ──────────────────────────────────────────────
# Build JSON paths array
PATHS_JSON="["
for i in "${!USE_PATHS[@]}"; do
    if [[ $i -gt 0 ]]; then
        PATHS_JSON+=","
    fi
    PATHS_JSON+="\"${USE_PATHS[$i]}\""
done
PATHS_JSON+="]"

cat > "${OUTPUT_DIR}/task_description.json" <<EOF
{
  "from_commit": "${FROM_COMMIT}",
  "to_commit": "${TO_COMMIT}",
  "paths": ${PATHS_JSON},
  "generated_at": "$(date -Iseconds)",
  "total_commits": ${TOTAL}
}
EOF

echo "==> Saved task_description.json"

# ─── Export diffs ─────────────────────────────────────────────────────────────
INDEX=0
for COMMIT in "${COMMITS[@]}"; do
    INDEX=$((INDEX + 1))
    PADDED=$(printf '%04d' "$INDEX")
    SHORT_SHA="${COMMIT:0:7}"
    DIFF_FILE="${OUTPUT_DIR}/${PADDED}_${SHORT_SHA}.diff"

    # git show with path filter: includes commit header + diff for specified paths only
    git show "$COMMIT" -- "${USE_PATHS[@]}" > "$DIFF_FILE"

    echo "    [${PADDED}/${TOTAL}] ${SHORT_SHA} -> $(basename "$DIFF_FILE")"
done

echo ""
echo "==> Done! ${TOTAL} diff(s) saved to ${OUTPUT_DIR}"
echo "==> Next step: use PROMPT.md with an LLM to generate the changelog."
