#!/bin/sh -eux

LOG_NAME="/opt/cron/export_to_mr_log_$YT_PROXY"

export_to_mr.py \
    --tables-queue //sys/cron/tables_to_export_to_mr \
    --mapreduce-binary /Berkanavt/bin/mapreduce \
    --skip-empty-tables \
    --force \
    >>$LOG_NAME 2>&1

cat $LOG_NAME | yt upload //sys/cron/export_to_mr_log
