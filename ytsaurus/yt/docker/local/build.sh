#!/usr/bin/env bash

script_name=$0

ytserver_all_path=./ytserver-all
ytserver_all_credits_path=../ytsaurus/credits/ytserver-all.CREDITS
yt_local_path=../../python/yt/local/bin/yt_local

image_tag=stable

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--ytserver-all ytserver_all_path]
                    [--ytserver-all-credits ytserver_all_credits_path]
                    [--yt-local yt_local_path]

  --ytserver-all: Path to ytserver-all binary (default: $ytserver_all_path)
  --ytserver-all-credits: Path to CREDITS file for ytserver-all binary (default: $ytserver_all_credits_path)
  --yt-local: Path to yt_local script (default: $yt_local_path)
  --image-tag: ytsaurus/local docker tag (default: $image_tag)

EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytserver-all)
        ytserver_all_path="$2"
        shift 2
        ;;
        --ytserver-all-credits)
        ytserver_all_credits_path="$2"
        shift 2
        ;;
        --yt-local)
        yt_local_path="$2"
        shift 2
        ;;
        --image-tag)
        image_tag="$2"
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

mkdir data
cp $yt_local_path data/yt_local
cp $ytserver_all_path data/ytserver-all
cp $ytserver_all_credits_path data/ytserver-all.CREDITS

docker build -t ytsaurus/local:$image_tag \
    --build-arg YT_LOCAL_PATH=data/yt_local \
    --build-arg YTSERVER_ALL_PATH=data/ytserver-all \
    --build-arg YTSERVER_ALL_CREDITS_PATH=data/ytserver-all.CREDITS \
    .

rm -rf data
