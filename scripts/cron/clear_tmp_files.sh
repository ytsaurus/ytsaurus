#!/bin/bash -eux

CLEAR_TMP="/opt/cron/clear_tmp.py"

$CLEAR_TMP --directory "//tmp/yt_wrapper/file_storage" --account "tmp_files" --max-node-count 50000 --safe-age "$((12 * 60))" --do-not-remove-objects-with-other-account --do-not-remove-objects-with-locks

