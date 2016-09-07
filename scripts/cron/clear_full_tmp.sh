#!/bin/bash -eux

CLEAR_TMP="/opt/cron/clear_tmp.py"

$CLEAR_TMP --directory "//tmp" --account "tmp" --do-not-remove-objects-with-other-account

