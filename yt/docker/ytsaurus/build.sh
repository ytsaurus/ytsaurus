#!/usr/bin/env bash

script_name=$0

ytserver_all_path=./ytserver-all
chyt_controller_path=./chyt-controller
clickhouse_trampoline_path=../../chyt/trampoline/clickhouse-trampoline.py
ytserver_clickhouse_path=./ytserver-clickhouse
ytserver_log_tailer_path=./ytserver-log-tailer
init_operation_archive_path=../../python/yt/environment/init_operation_archive.py

driver_bindings_package_path=./ytsaurus_native_driver-1.0.0-cp39-cp39-linux_x86_64.whl

ytserver_all_credits_path=./ytserver-all.CREDITS.txt
chyt_controller_credits_path=./chyt-controller.CREDITS.txt
ytserver_clickhouse_credits_path=./ytserver-clickhouse.CREDITS.txt
ytserver_log_tailer_credits_path=./ytserver-log-tailer.CREDITS.txt

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--ytserver-all ytserver_all_path]
                    [--yt-local yt_local_path]

  --ytserver-all: Path to ytserver-all binary (default: $ytserver_all_path)
  --ytserver-all-credits: Path to CREDITS file for ytserver-all binary (default: $ytserver_all_credits_path)
  --chyt-controller: Path to CHYT controller binary (default: $chyt_controller_path)
  --chyt-controller-credits: Path to CREDITS file for CHYT controller binary (default: $chyt_controller_credits_path)
  --clickhouse-trampoline: Path to clickhouse trampoline script (default: $clickhouse_trampoline_path)
  --ytserver-clickhouse: Path to ytserver-clickhouse binary (default: $ytserver_clickhouse_path)
  --ytserver-clickhouse-credits: Path to CREDITS file for ytserver-clickhouse binary (default: $ytserver_clickhouse_credits_path)
  --ytserver-log-tailer: Path to ytserver-log-tailer binary (default: $ytserver_log_tailer_path)
  --ytserver-log-tailer-credits: Path to CREDITS file for ytserver-log-tailer binary (default: $ytserver_log_tailer_credits_path)
  --init-operation-archive: Path to init_operation_archive script (default: $init_operation_archive_path)
  --driver-bindings-package: Path to built python driver_bindings package (default: $driver_bindings_package_path)

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
        --chyt-controller)
        chyt_controller_path="$2"
        shift 2
        ;;
        --chyt-controller-credits)
        chyt_controller_credits_path="$2"
        shift 2
        ;;
        --clickhouse-trampoline)
        clickhouse_trampoline_path="$2"
        shift 2
        ;;
        --ytserver-clickhouse)
        ytserver_clickhouse_path="$2"
        shift 2
        ;;
        --ytserver-clickhouse-credits)
        ytserver_clickhouse_credits_path="$2"
        shift 2
        ;;
        --ytserver-log-tailer)
        ytserver_log_tailer_path="$2"
        shift 2
        ;;
        --ytserver-log-tailer-credits)
        ytserver_log_tailer_credits_path="$2"
        shift 2
        ;;
        --init-operation-archive)
        init_operation_archive_path="$2"
        shift 2
        ;;
        --driver-bindings-package)
        driver_bindings_package_path="$2"
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
cp $ytserver_all_path data/ytserver-all
cp $ytserver_all_credits_path data/ytserver-all.CREDITS
cp $chyt_controller_path data/chyt-controller
cp $chyt_controller_credits_path data/chyt-controller.CREDITS
cp $clickhouse_trampoline_path data/clickhouse-trampoline
cp $ytserver_clickhouse_path data/ytserver-clickhouse
cp $ytserver_clickhouse_credits_path data/ytserver-clickhouse.CREDITS
cp $ytserver_log_tailer_path data/ytserver-log-tailer
cp $ytserver_log_tailer_credits_path data/ytserver-log-tailer.CREDITS
cp $init_operation_archive_path data/init_operation_archive
cp $driver_bindings_package_path data/ytsaurus_native_driver-1.0.0-cp39-cp39-linux_x86_64.whl

docker build -t ytsaurus/ytsaurus:unstable-0.0.2 \
    --build-arg YTSERVER_ALL_PATH=data/ytserver-all \
    --build-arg YTSERVER_ALL_CREDITS_PATH=data/ytserver-all.CREDITS \
    --build-arg CHYT_CONTROLLER_PATH=data/chyt-controller \
    --build-arg CHYT_CONTROLLER_CREDITS_PATH=data/chyt-controller.CREDITS \
    --build-arg CLICKHOUSE_TRAMPOLINE_PATH=data/clickhouse-trampoline \
    --build-arg YTSERVER_CLICKHOUSE_PATH=data/ytserver-clickhouse \
    --build-arg YTSERVER_CLICKHOUSE_CREDITS_PATH=data/ytserver-clickhouse.CREDITS \
    --build-arg YTSERVER_LOG_TAILER_PATH=data/ytserver-log-tailer \
    --build-arg YTSERVER_LOG_TAILER_CREDITS_PATH=data/ytserver-log-tailer.CREDITS \
    --build-arg INIT_OPERATION_ARCHIVE_PATH=data/init_operation_archive \
    --build-arg DRIVER_BINDINGS_PACKAGE_PATH=data/ytsaurus_native_driver-1.0.0-cp39-cp39-linux_x86_64.whl \
    .

rm -rf data
