#!/bin/sh -eux

export_to_yt.py --tables-queue //sys/cron/tables_to_export_to_yt --yt-token $YT_TOKEN --fastbone >>/opt/cron/export_to_yt_log

cat /opt/cron/export_to_yt_log | yt upload //sys/cron/export_to_yt_log

