#!/bin/bash -eux

CLEAR_TMP="/opt/cron/clear_tmp.py"

$CLEAR_TMP --directory "//tmp/trash" --max-disk-space $((1024 * 1024 * 1024 * 1024 * 1024))

