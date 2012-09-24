#!/bin/bash

source=$1
target=$2

codec=$3

[[ -z $source ]] && exit 1
[[ -z $target ]] && exit 2

[[ -z $codec ]] && codec=lz4

echo "--- Copying '$source' to '$target' (compressing with $codec)"

function keep_tx() {
    sleep 5
    if ./yt renew_tx --config yt.conf "$1" > /dev/null 2>&1 ; then
        keep_tx $1 &
    else
        echo "--- Giving up on transaction $1" 2>&1
    fi
}

echo "--- Removing '$target'" >&2
./yt remove --config yt.conf "$target"

echo "--- Creating '$target'" >&2
./yt create --config yt.conf table "$target"

tx=$(./yt start_tx --config yt.conf --format dsv --opt /timeout=120000)
if [[ "$tx" == "0-0-0-0" ]]; then
    echo "--- Failed to start transaction"
    exit 1
fi

keep_tx $tx &
echo "--- Working within transaction '$tx'" >&2

time MR_USER=tmp \
    ./mapreduce -server "redwood.yandex.ru:8013" \
    -src "$source" -dst tmp/null \
    -subkey -lenval \
    -file ./libc.so.6 \
    -file ./libdl.so.2 \
    -file ./libgcc_s.so.1 \
    -file ./libm.so.6 \
    -file ./libpthread.so.0 \
    -file ./librt.so.1 \
    -file ./libstdc++.so.6 \
    -file ./libyajl.so.2 \
    -file ./libytext-json.so \
    -file ./libytext-uv.so.0.6 \
    -file ./yt \
    -file ./yt.bin \
    -file ./yt.conf \
    -map "bash yt write --config yt.conf --config_opt \"/table_writer/codec_id=$codec\" --tx \"$tx\" --format \"<lenval=true;has_subkey=true>yamr\" '$target'"

success=$?

src_row_count=$(\
    wget -qO - "http://redwood.yandex.ru:13013/debug?info=table&table=$source" \
    | grep '<b>Records:' \
    | perl -pe 's/^<b>Records:\s*<\/b>([\d,]+).*$/\1/;s/,//g' \
)
dst_row_count=$(
    ./yt get --config yt.conf --tx "$tx" --format dsv "$target/@row_count"
)

[[ -z "$src_row_count" ]] && src_row_count=-1
[[ -z "$dst_row_count" ]] && dst_row_count=-1

if [[ "$src_row_count" = "$dst_row_count" ]]; then
    match=1
else
    match=0
fi

echo "*** source=$source;target=$target;src_row_count=$src_row_count;dst_row_count=$dst_row_count;match=$match" >&2

if [[ "$success" -eq "0" ]]; then
    echo "--- Commiting $tx" >&2
    ./yt commit_tx --config yt.conf "$tx"
else
    echo "--- Aborting $tx" >&2
    ./yt abort_tx  --config yt.conf "$tx"
fi

