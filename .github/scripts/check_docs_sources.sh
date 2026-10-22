#!/bin/bash
set -euo pipefail

root=${1:?usage: check_docs_sources.sh <repository-root>}
ya_make="$root/yt/docs/ya.make"
spyt_examples=yt/spark/spark-over-yt/examples/java/src/main/java/tech/ytsaurus/spyt/example
pinned_spyt_hashes="797407b1068970f5ba7b32b7808552df242233e64f283c9a0eb1affcef1b66e0  $spyt_examples/SmokeTest.java
17154d3b35c1754157b307e01862d55e1f1fd7c5c9711202c33cc802aa0f162c  $spyt_examples/UdfExample.java
d824554977e6c304f71c73bf6c50940d14e5806ac5c3aa698f2089afd5e04f04  $spyt_examples/GroupingExample.java"

block_count=$(grep -c '^DOCS_INCLUDE_SOURCES(' "$ya_make" || true)
if [ "$block_count" -ne 1 ]; then
    echo "Expected exactly one DOCS_INCLUDE_SOURCES block in $ya_make, found $block_count" >&2
    exit 1
fi

declared=$(awk '
    /^DOCS_INCLUDE_SOURCES\(/ { inside = 1; next }
    inside && /^\)/ { closed = 1; exit }
    inside { sub(/#.*/, ""); for (i = 1; i <= NF; i++) print $i }
    END { if (!closed) exit 1 }
' "$ya_make") || {
    echo "Unterminated DOCS_INCLUDE_SOURCES block in $ya_make" >&2
    exit 1
}
mapfile -t sources <<< "$declared"

if [ -z "$declared" ]; then
    echo "No declared sources in $ya_make" >&2
    exit 1
fi

declare -A seen
failures=0
for source in "${sources[@]}"; do
    if [[ "$source" == /* || "/$source/" == */../* ]]; then
        echo "Declared source must be repository-relative: $source" >&2
        failures=$((failures + 1))
    elif [[ -n "${seen[$source]:-}" ]]; then
        echo "Duplicate declared source: $source" >&2
        failures=$((failures + 1))
    elif [ ! -f "$root/$source" ]; then
        echo "Missing declared source: $source" >&2
        failures=$((failures + 1))
    fi
    seen[$source]=1
done

while read -r _ spyt_source; do
    if [[ -z "${seen[$spyt_source]:-}" ]]; then
        echo "Pinned SPYT source is not declared: $spyt_source" >&2
        failures=$((failures + 1))
    fi
done <<< "$pinned_spyt_hashes"

if [ "$failures" -ne 0 ]; then
    echo "Docs source check failed with $failures error(s)" >&2
    exit 1
fi

if ! (cd "$root" && sha256sum --check --strict --quiet <<< "$pinned_spyt_hashes"); then
    echo "Pinned SPYT source hash check failed" >&2
    exit 1
fi

echo "Verified ${#sources[@]} declared sources and 3 pinned SPYT hashes"
