#!/usr/bin/env bash

script_name=$0

build_output=../../../build_output
publish_scripts=../publisher

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--build-output build_output]
                    [--publish-scripts publish_scripts]
                    [--spyt-version spyt_version]
                    [--spark-version spark_version]

  --build-output: Path to built spyt components (default: $build_output)
  --publish-scripts: Path to spyt publish scripts (default: $publish_scripts)
  --spyt-version: Spyt version
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
        --publish-scripts)
        publish_scripts="$2"
        shift 2
        ;;
        --spyt-version)
        spyt_version="$2"
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

if [[ -z "$spark_version" || -z "$spyt_version" ]]; then
  echo "No version specified"
  exit 1
fi

mkdir data
cp -rL $build_output/* data/
mkdir scripts
cp -rL $publish_scripts/* scripts/

docker build \
    -t ytsaurus/spyt:"$spyt_version" \
    --build-arg BUILD_OUTPUT_PATH=data \
    --build-arg SPARK_VERSION=$spark_version \
    --build-arg PUBLISH_SCRIPTS_PATH=scripts \
    .

rm -rf data
rm -rf scripts
