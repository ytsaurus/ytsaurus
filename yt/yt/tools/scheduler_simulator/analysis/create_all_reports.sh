#!/usr/bin/env bash
set -eu

TARGET="$1"

result="$(find "$TARGET" | grep data$)"

for line in "$result"
do
    arg="${line%/*}"
    create_reports.sh "$arg"
done
