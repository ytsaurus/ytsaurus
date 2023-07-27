#!/usr/bin/env bash
set -eu

if [[ "1" != "$#" ]]; then
    echo "Usage: convert_to_binary_format COMPRESSED_OPERATIONS_LOG_WITHOUT_EXTENSION"
    exit 1
fi

echo "Decompressing $1" >&2

simulator_operation_decompressor --input "$1.compressed.yson" --output "$1.yson"

echo "Converting $1 to binary format" >&2

cat "$1.yson" | convert_operations_to_binary_format "$1.bin"
