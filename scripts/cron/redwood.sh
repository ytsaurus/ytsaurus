#!/bin/sh -eux

set +u
if [ -z "$USER_SESSIONS_PERIOD" ]; then
    echo "You must specify USER_SESSIONS_PERIOD" >&2
    exit 1
fi
if [ -z "$USER_SESSIONS_FRAUDS_PERIOD" ]; then
    USER_SESSIONS_FRAUDS_PERIOD="$USER_SESSIONS_PERIOD"
    USER_SESSIONS_SPY_LOG_PERIOD="$USER_SESSIONS_PERIOD"
fi
if [ -z "$FAST" ]; then
    FAST=0
fi
set -u

IMPORT_PATH="//userdata"
IMPORT_QUEUE="//sys/cron/tables_to_import_from_redwood"
REMOVE_QUEUE="//sys/cron/tables_to_remove"
LINK_QUEUE="//sys/cron/link_tasks"
LOCK_PATH="//sys/cron/redwood_lock"

/opt/cron/redwood.py \
    --path $IMPORT_PATH \
    --import-queue $IMPORT_QUEUE \
    --remove-queue $REMOVE_QUEUE \
    --link-queue $LINK_QUEUE \
    --user-sessions-period $USER_SESSIONS_PERIOD \
    --user-sessions-frauds-period $USER_SESSIONS_FRAUDS_PERIOD \
    --user-sessions-spy-log-period $USER_SESSIONS_SPY_LOG_PERIOD

if [ "$FAST" != 0 ]; then
    /opt/cron/redwood_fast.py \
        --path $IMPORT_PATH \
        --import-queue $IMPORT_QUEUE \
        --remove-queue $REMOVE_QUEUE
fi

/opt/cron/tools/remove.py $REMOVE_QUEUE

import_from_mr.py \
    --tables-queue "$IMPORT_QUEUE" \
    --destination-dir "$IMPORT_PATH" \
    --mapreduce-binary "/Berkanavt/bin/mapreduce" \
    --mr-server "redwood00.search.yandex.net" \
    --compression-codec "gzip_best_compression" --erasure-codec "lrc_12_2_2" \
    --yt-pool "redwood_restricted" \
    --fastbone

/opt/cron/tools/link.py $LINK_QUEUE
