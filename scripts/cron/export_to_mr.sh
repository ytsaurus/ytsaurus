#!/bin/sh -eux

export_to_mr.py --tables-queue //sys/cron/tables_to_export_to_mr --mapreduce-binary /opt/cron/tools/mapreduce --force >>/opt/cron/export_to_mr_log

cat /opt/cron/export_to_mr_log | yt upload //sys/cron/export_to_mr_log
