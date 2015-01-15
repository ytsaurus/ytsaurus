#!/bin/bash -eux

QUEUE="//sys/cron/tables_to_compress"

/opt/cron/tools/compress.py find --queue $QUEUE

/opt/cron/tools/run_parallel.sh "/opt/cron/tools/compress.py run --queue $QUEUE" 10 "/opt/cron/compression_log_$YT_PROXY"


