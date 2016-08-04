#!/bin/bash -eux

CLEAR_TMP="/opt/cron/clear_tmp.py"

$CLEAR_TMP --directory "//tmp" --account "tmp" --do-not-remove-objects-with-other-account
$CLEAR_TMP --directory "//tmp/yt_wrapper/file_storage" --account "tmp_files" --do-not-remove-objects-with-other-account
