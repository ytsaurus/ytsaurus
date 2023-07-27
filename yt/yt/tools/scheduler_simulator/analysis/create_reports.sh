#!/usr/bin/env bash
set -eu

OUTPUT_PATH="${1%/}"

echo "Generating reports for $OUTPUT_PATH"

if [ -f "$OUTPUT_PATH/data.tar.gz" ]; then
    tar -zxvf "$OUTPUT_PATH/data.tar.gz"
fi

export data_dir="$OUTPUT_PATH/data/"
jupyter nbconvert --execute \
    --to html \
    --ExecutePreprocessor.timeout=600 \
    --output-dir="$OUTPUT_PATH" \
    Analyze_simulation.ipynb

for target in pools operations
do
    export target="$target"
    jupyter nbconvert --execute \
        --to html \
        --ExecutePreprocessor.timeout=600 \
        --output-dir="$OUTPUT_PATH" \
        Measure_fairness.ipynb
done

#~/yt/scheduler_simulator/archive.sh $OUTPUT_PATH/data
