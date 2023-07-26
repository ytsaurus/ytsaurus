#!/usr/bin/env bash

script_name=$0

build_output=../../../build_output

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--build-output build_output]
                    [--spark-version spark_version]

  --build-output: Path to built spyt components (default: $build_output)
  --spark-version: Spark fork version

EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --build-output)
        build_output="$2"
        shift 2
        ;;
        --spark-version)
        spark_version="$2"
        shift 2
        ;;
        -h|--help)
        print_usage
        shift
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

if [[ -z "$spark_version" ]]; then
  echo "No version specified"
  exit 1
fi

mkdir data
cp -rL $build_output/* data/

docker build \
    -t ytsaurus/spark:"$spark_version" \
    --build-arg BUILD_OUTPUT_PATH=data \
    .

rm -rf data
