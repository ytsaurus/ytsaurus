#!/bin/bash

# YT-specific synchronization helper. Unlike the shared YQL helper, it accepts
# an explicit source revision so that a new weekly PR can be stacked on an open
# previous PR while importing only the next YQL documentation interval.

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <target_info_file> <source_revision>" >&2
    exit 2
fi

TARGET_INFO_FILE="$1"
SOURCE_REVISION="$2"

if [ ! -f "$TARGET_INFO_FILE" ]; then
    echo "Target info file $TARGET_INFO_FILE does not exist" >&2
    exit 1
fi

TARGET_INFO_FILE="$(realpath "$TARGET_INFO_FILE")"
IFS=';' read -r BASE_REV ARC_FROM < "$TARGET_INFO_FILE"
TO=$(dirname "$TARGET_INFO_FILE")
ARC_ROOT=$(cd "$TO" && arc root)
HEAD_REV=$(cd "$TO" && arc rev-parse "$SOURCE_REVISION")
ARC_TO=${TO#"$ARC_ROOT"/}
DATETIME=$(date '+%Y-%m-%d-%H-%M-%S')

if [ -z "$BASE_REV" ] || [ -z "$ARC_FROM" ]; then
    echo "Target info file must contain <base_revision>;<source_path>" >&2
    exit 1
fi

if [ -n "$(cd "$ARC_ROOT" && arc status -s -u all)" ]; then
    echo "Arc workspace must be clean before YQL documentation synchronization" >&2
    exit 1
fi

echo "Base revision: $BASE_REV"
echo "Source revision: $HEAD_REV"
echo "Arc root: $ARC_ROOT"
echo "Source: $ARC_FROM"
echo "Target: $ARC_TO"

PATCH_FILE=$(mktemp)
BASE_EXPORT_DIR=$(mktemp -d)
CURRENT_EXPORT_DIR=$(mktemp -d)

cleanup() {
    rm -f "$PATCH_FILE"
    rm -rf "$BASE_EXPORT_DIR" "$CURRENT_EXPORT_DIR"
}
trap cleanup EXIT

cd "$ARC_ROOT"

echo "Use $BASE_EXPORT_DIR base source export dir"
arc export "$BASE_REV" "$ARC_FROM" --to "$BASE_EXPORT_DIR"
rsync -r --delete --filter='. -' "$BASE_EXPORT_DIR/$ARC_FROM/" "$TO" << 'EOF'
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

arc add -A "$ARC_TO"
arc diff --cached --reverse --relative="$ARC_TO" > "$PATCH_FILE"
arc reset --hard
arc clean -d

echo "Use $CURRENT_EXPORT_DIR current source export dir"
arc export "$HEAD_REV" "$ARC_FROM" --to "$CURRENT_EXPORT_DIR"
rsync -r --delete --filter='. -' "$CURRENT_EXPORT_DIR/$ARC_FROM/" "$TO" << 'EOF'
+ /*/
+ *.md
+ toc_*.yaml
P _assets/*
- /*
EOF

patch -d "$TO" -p0 -N -E --no-backup-if-mismatch --merge -i "$PATCH_FILE" -t || \
    echo "Patch has conflicts. Consider reviewing them before commit"

if [ -n "$(arc status -s -u all "$ARC_TO")" ]; then
    printf '%s;%s\n' "$HEAD_REV" "$ARC_FROM" > "$TARGET_INFO_FILE"
else
    echo "Nothing changed"
fi

if [ "${KEEP_PATCH+x}" = "x" ]; then
    mv "$PATCH_FILE" "$TO/$DATETIME.patch"
fi
