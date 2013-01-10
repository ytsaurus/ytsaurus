#!/bin/bash

set -o pipefail
set -e

while getopts "f:" opt; do
    case $opt in
        f)
            format=$OPTARG
        ;;
        \?)
            exit 1
        ;;
    esac
done

shift $((OPTIND-1))


source=$1
target=$2
codec=${format-$3}

TOTAL_RATE_LIMIT=1073741824 #1GB/s
MAX_TRANSFER_STREAMS=50

[[ -z $source ]] && exit 1
[[ -z $target ]] && exit 1
[[ -z $codec ]] && codec=lz4

. ./ytface.sh

function keep_tx() {
    sleep 5
    while renew_tx $1 >/dev/null 2>&1; do
        sleep 5
    done
}

function datestamp {
    date +%Y-%m-%dT%H:%M:%S.%N
}
function log {
    echo "$(datestamp) $1"
}
function e {
    log "-- $*"
    $*
}

function mr_table_rowcount {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep recordCount|grep -o -e '[0-9]\+'
}

function mr_table_chunkcount {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep chunkCount|grep -o -e '[0-9]\+'
}

function mr_table_size {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep size|grep -o -e '[0-9]\+'
}

function mr_table_is_sorted {
    local sorted=$(curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep sorted|grep -o 'yes\|no') 
    if [ "$sorted" = "yes" ]; then
        echo 1
    else
        echo 0
    fi
}

function min {
    if [ $1 -lt $2 ]; then echo $1; else echo $2; fi
}

source_chunkcount=$(mr_table_chunkcount $source)
jobcount=$(min $source_chunkcount $MAX_TRANSFER_STREAMS)
jobratelimit=$(($TOTAL_RATE_LIMIT / $jobcount))

source_is_sorted=$(mr_table_is_sorted $source)

log "Copying '$source' to '$target' (sorted=$source_is_sorted, codec=$codec)"

tx=$(start_tx 10000)
if [[ "$tx" == "0-0-0-0" ]]; then
    log "-- Failed to start transaction"
    exit 1
fi

# kill transaction keeper on exit or termination.
# another variant would be "kill 0" to kill entire process group
# but that would kill not only children of this process but also
# parents of this process, which is not what we want
trap 'kill %keep_tx' SIGINT SIGTERM EXIT
keep_tx $tx &

log "-- Working within transaction $tx"

e drop_table $target $tx

e create_table $target $tx

function push_table_hosts2hosts {
    local source=$1
    local target=$2
    local tx=$3
    local codec=$4
    ./mapreduce \
        -server "${MRSERVER}:8013" \
        -opt user=tmp \
        -src "$source" -dst tmp/null \
        -subkey -lenval \
        -opt jobcount=$jobcount \
        -opt threadcount=1 \
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
        -file ./yt.conf \
        -map "LD_LIBRARY_PATH=. ./yt write --config yt.conf --config_opt \"/table_writer/codec_id=$codec\" --tx $tx --format \"<lenval=true;has_subkey=true>yamr\" \"$target\""
}
function push_table_hosts2proxies_mapreduce_yt {
    local source=$1
    local target=$2
    local tx=$3
    local codec=$4
    ./mapreduce \
        -server "${MRSERVER}:8013" \
        -opt user=tmp \
        -src "$source" -dst tmp/null \
        -subkey -lenval \
        -opt jobcount=$jobcount \
        -opt threadcount=1 \
        -memlimit 5000 \
        -file ./mapreduce-yt \
        -map "./mapreduce-yt -subkey -lenval -write $target -tx $tx -codec $codec"
}
function push_table_hosts2proxies_raw_curl {
    local source=$1
    local target=$2
    local tx=$3
    local codec=$4
    ./mapreduce \
        -server "${MRSERVER}:8013" \
        -opt user=tmp \
        -src "$source" -dst tmp/null \
        -subkey -lenval \
        -opt jobcount=$jobcount \
        -opt threadcount=1 \
        -map "gzip -9 -c | curl -s -X PUT \
            -H \"Accept: application/x-yt-yson-pretty\" \
            -H \"Content-Type: application/x-yamr-subkey-lenval\" \
            -H \"Transfer-Encoding: chunked\" \
            -H \"Content-Encoding: gzip\" \
            --limit-rate $jobratelimit \
            --globoff \
            -T- \"http://proxy.yt.yandex.net/api/write?path=$target&transaction_id=$tx&table_writer[codec_id]=$codec\" \
            "
}
function push_table_hosts2proxies_curl {
    local source=$1
    local target=$2
    local tx=$3
    local codec=$4
    ./mapreduce \
        -server "${MRSERVER}:8013" \
        -opt user=tmp \
        -src "$source" -dst tmp/null \
        -subkey \
        -lenval \
        -opt jobcount=$jobcount \
        -opt threadcount=1 \
        -file ytface.sh \
        -file push-little-ones-push \
        -file mr-split \
        -file profile_support.py \
        -map "bash -c 'FASTBONE=${FASTBONE} ./push-little-ones-push -S $target $tx $codec $jobratelimit >&2'"
}
function push_table_hosts2proxies_splitter_curl {
    local source=$1
    local target=$2
    local tx=$3
    local codec=$4
    ./mapreduce \
        -server "${MRSERVER}:8013" \
        -opt user=tmp \
        -src "$source" -dst tmp/null \
        -subkey \
        -lenval \
        -opt jobcount=$jobcount \
        -opt threadcount=1 \
        -file ytface.sh \
        -file push-little-ones-push \
        -file mr-split \
        -file profile_support.py \
        -map "bash -c 'FASTBONE=${FASTBONE} ./push-little-ones-push $target $tx $codec $jobratelimit >&2'"
}

timemark=$(date +%s)

#e push_table_hosts2hosts "$source" "$target" "$tx" "$codec"
#e push_table_hosts2proxies_mapreduce_yt "$source" "$target" "$tx" "$codec"
#e push_table_hosts2proxies_raw_curl "$source" "$target" "$tx" "$codec"
#e push_table_hosts2proxies_curl "$source" "$target" "$tx" "$codec"
e push_table_hosts2proxies_splitter_curl "$source" "$target" "$tx" "$codec"

elapsed=$(($(date +%s) - $timemark))

# this byte size is a lie, actually, but close enough
compressed_size=$(table_attr $target compressed_data_size $tx)
log "-- transfer result: bytes=$compressed_size, time=$elapsed s, rate=$(($compressed_size/$elapsed)) B/s"

src_rowcount=$(mr_table_rowcount $source)
dst_rowcount=$(table_rowcount $target $tx)

if [ "$src_rowcount" != "$dst_rowcount" ]; then
    log "-- Copy failed: row count mismatch: src=$src_rowcount, dst=$dst_rowcount"
    e abort_tx $tx
    exit 1
fi


src_size=$(mr_table_size $source)
#dst_size=$(table_attr size $tx)
log "-- Copy ok: rows=$src_rowcount, raw_bytes=$src_size"

if [ $source_is_sorted == 1 ] ; then
    timemark=$(date +%s)
    e sort_table $target $target $tx $codec
    log "-- Sort: time=$(($(date +%s) - $timemark)) seconds"
fi

e commit_tx $tx

