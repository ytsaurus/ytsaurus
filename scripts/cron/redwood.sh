#!/bin/sh -eux

export YT_PROXY="kant.yt.yandex.net"

IMPORT_PATH="//userdata"
IMPORT_QUEUE="//sys/cron/tables_to_import_from_redwood"
REMOVE_QUEUE="//sys/cron/tables_to_remove"

/opt/cron/redwood.py --path $IMPORT_PATH --import-queue $IMPORT_QUEUE --remove-queue $REMOVE_QUEUE

/opt/cron/tools/remove.py $REMOVE_QUEUE

/opt/cron/tools/import_table_from_mr.py \
    --tables "$IMPORT_QUEUE" --destination="$IMPORT_PATH" \
    --server "redwood.yandex.ru" --mapreduce "/opt/cron/tools/mapreduce" \
    --pool "redwood_restricted" --codec "gzip_best_compression" \
    --use-fastbone --ignore
