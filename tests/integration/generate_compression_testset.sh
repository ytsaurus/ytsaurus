#!/bin/bash -e

export YT_PROXY="locke.yt.yandex.net"
TEST_DATA_PATH="//home/files/test_data/compression"
RUN_CODEC="run_codec"
YT="yt2"

codecs=(
    'snappy'
    'zlib6'
    'zlib9'
    'lz4'
    'lz4_high_compression'
    'quick_lz'
    'zstd'
    'brotli3'
    'brotli5'
    'brotli8'
)

function print_usage() {
    echo "usage: $0 DATA"
}

if [ -z "$1" ]
then
    echo "No data supplied"
    print_usage
    exit 1
fi

DATA=$1

function generate_tests() {
    echo "Generating compression test data from" \"$DATA\"

    $YT create -ri map_node $TEST_DATA_PATH
    cat $DATA | $YT upload $TEST_DATA_PATH/$DATA
    for CODEC in ${codecs[*]}
    do
        echo "Upload data for codec" $CODEC
        cat $DATA | $RUN_CODEC compress $CODEC | $YT upload $TEST_DATA_PATH/$DATA.compressed.$CODEC
    done
}

generate_tests
