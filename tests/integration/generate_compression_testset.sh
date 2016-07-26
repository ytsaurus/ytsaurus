#!/bin/bash -e

export YT_PROXY="locke.yt.yandex.net"
TEST_DATA_PATH="//home/files/test_data/compression"
RUN_CODEC="run_codec"
YT="yt2"

codecs=(
    "snappy"
    "lz4"
    "lz4_high_compression"
    "quick_lz"
    "brotli_1"
    "brotli_2"
    "brotli_3"
    "brotli_4"
    "brotli_5"
    "brotli_6"
    "brotli_7"
    "brotli_8"
    "brotli_9"
    "brotli_10"
    "brotli_11"
    "zlib_1"
    "zlib_2"
    "zlib_3"
    "zlib_4"
    "zlib_5"
    "zlib_6"
    "zlib_7"
    "zlib_8"
    "zlib_9"
    "zstd_1"
    "zstd_2"
    "zstd_3"
    "zstd_4"
    "zstd_5"
    "zstd_6"
    "zstd_7"
    "zstd_8"
    "zstd_9"
    "zstd_10"
    "zstd_11"
    "zstd_12"
    "zstd_13"
    "zstd_14"
    "zstd_15"
    "zstd_16"
    "zstd_17"
    "zstd_18"
    "zstd_19"
    "zstd_20"
    "zstd_21"
    "zstd_legacy"
)

function print_usage() {
    echo "usage: $0 TESTSET_NAME DATA"
}

if [ -z "$1" ]
then
    echo "No testset name supplied"
    print_usage
    exit 1
fi

if [ -z "$2" ]
then
    echo "No data supplied"
    print_usage
    exit 1
fi

TESTSET_NAME=$1
DATA=$2

function generate_tests() {
    echo "Generating compression testset" \"$TESTSET_NAME\" "from" \"$DATA\"

    TESTSET_PATH=$TEST_DATA_PATH/$TESTSET_NAME
    $YT create -ri map_node $TESTSET_PATH
    cat $DATA | $YT upload $TESTSET_PATH/$DATA
    for CODEC in ${codecs[*]}
    do
        echo "Upload data for codec" $CODEC
        cat $DATA | $RUN_CODEC compress $CODEC | $YT upload $TESTSET_PATH/$DATA.compressed.$CODEC
    done
}

generate_tests
