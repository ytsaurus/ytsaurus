#!/bin/sh -eux

IMPORT_QUEUE="//sys/cron/tables_to_import"

import_from_mr.py \
    --tables-queue "$IMPORT_QUEUE" \
    --mapreduce-binary "/Berkanavt/bin/mapreduce" \
    --compression-codec "gzip_best_compression" \
    --yt-pool "restricted" \
    --skip-empty-tables \
    --fastbone

cat /opt/cron/import_log | yt upload //sys/cron/import_log
