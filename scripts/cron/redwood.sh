#!/bin/sh -eux

export YT_PROXY="kant.yt.yandex.net"

IMPORT_PATH="//userdata"
IMPORT_QUEUE="//sys/cron/tables_to_import_from_redwood"
REMOVE_QUEUE="//sys/cron/tables_to_remove"
LOCK_PATH="//sys/cron/redwood_lock"

/opt/cron/redwood.py --path $IMPORT_PATH --import-queue $IMPORT_QUEUE --remove-queue $REMOVE_QUEUE

/opt/cron/tools/remove.py $REMOVE_QUEUE

/opt/cron/tools/import_table_from_mr.py \
    --mapreduce-binary "/opt/cron/tools/mapreduce" \
    --tables "$IMPORT_QUEUE" --destination="$IMPORT_PATH" \
    --server "redwood.yandex.ru" --pool "redwood_restricted" \
    --compression-codec "gzip_best_compression" --erasure-codec "lrc_12_2_2" \
    --lock "$LOCK_PATH" \
    --mapreduce-binary "/opt/cron/tools/mapreduce" \
    --fastbone --ignore
