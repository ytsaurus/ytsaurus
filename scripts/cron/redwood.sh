#!/bin/sh -eux

export YT_PROXY="kant.yt.yandex.net"

IMPORT_PATH="//userdata"
IMPORT_QUEUE="//sys/cron/tables_to_import_from_redwood"
REMOVE_QUEUE="//sys/cron/tables_to_remove"
LOCK_PATH="//sys/cron/redwood_lock"

/opt/cron/redwood.py --path $IMPORT_PATH --import-queue $IMPORT_QUEUE --remove-queue $REMOVE_QUEUE

/opt/cron/tools/remove.py $REMOVE_QUEUE

import_from_mr.py \
    --tables-queue "$IMPORT_QUEUE" \
    --destination-dir "$IMPORT_PATH" \
    --mapreduce-binary "/opt/cron/tools/mapreduce" \
    --mr-server "redwood.yandex.ru" \
    --compression-codec "gzip_best_compression" --erasure-codec "lrc_12_2_2" \
    --yt-pool "redwood_restricted" \
    --fastbone
    
#--lock "$LOCK_PATH" \
