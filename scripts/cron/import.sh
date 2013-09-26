#!/bin/sh -eux

export YT_PROXY="kant.yt.yandex.net"

IMPORT_QUEUE="//sys/cron/tables_to_import"
LOCK_PATH="//sys/cron/import_lock"

import_from_mr.py \
    --tables-queue "$IMPORT_QUEUE" \
    --mapreduce-binary "/opt/cron/tools/mapreduce" \
    --compression-codec "gzip_best_compression" \
    --yt-pool "restricted" \
    --fastbone

#--lock "$LOCK_PATH" \
