#!/bin/sh -eux

LOG_NAME="/opt/cron/export_to_yt_log_$YT_PROXY"

export_to_yt.py --tables-queue //sys/cron/tables_to_export_to_yt --yt-token $YT_TOKEN --fastbone >>$LOG_NAME 2>&1

cat $LOG_NAME | yt upload //sys/cron/export_to_yt_log

