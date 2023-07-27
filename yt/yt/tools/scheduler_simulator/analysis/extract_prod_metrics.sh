#!/usr/bin/env bash
set -eu

INPUT_DIR="${1%/}"
OUTPUT_DIR="${2%/}"

mkdir -p "$OUTPUT_DIR"

echo "Input dir: $INPUT_DIR"
echo "Output dir: $OUTPUT_DIR"

mkdir -p "$OUTPUT_DIR/data"

cp "$INPUT_DIR"/* "$OUTPUT_DIR/data/"
