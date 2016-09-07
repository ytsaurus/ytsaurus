#!/bin/bash -eux

CLEAR_TMP="/opt/cron/clear_tmp.py"

$CLEAR_TMP --directory "//tmp/yt_wrapper/file_storage" --account "tmp_files" --max-node-count 40000 --do-not-remove-objects-with-other-account

