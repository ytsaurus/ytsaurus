if ! [[ -v USER_SESSIONS_TR_PERIOD ]]; then
    echo "You must specify USER_SESSIONS_TR_PERIOD" >&2
    exit 1
fi

IMPORT_PATH="//userdata"
IMPORT_QUEUE="//sys/cron/tables_to_import_from_sakura"
REMOVE_QUEUE="//sys/cron/tables_to_remove2"
LINK_QUEUE="//sys/cron/link_tasks2"

/opt/cron/sakura_tr.py \
    --path $IMPORT_PATH \
    --import-queue $IMPORT_QUEUE \
    --remove-queue $REMOVE_QUEUE \
    --link-queue $LINK_QUEUE \
    --user-sessions-period $USER_SESSIONS_TR_PERIOD

IMPORT_COMMAND='
import_from_mr.py
    --tables-queue '"$IMPORT_QUEUE"'
    --destination-dir '"$IMPORT_PATH"'
    --mapreduce-binary /Berkanavt/bin/mapreduce-dev
    --mr-server sakura00.search.yandex.net
    --compression-codec gzip_best_compression
    --erasure-codec lrc_12_2_2
    --yt-pool sakura_restricted
    --fastbone
'

/opt/cron/tools/remove.py $REMOVE_QUEUE

/opt/cron/tools/run_parallel.sh "$IMPORT_COMMAND" 4 "/dev/stdout"

/opt/cron/tools/link.py $LINK_QUEUE
