#!/bin/bash -eux

QUEUE="//sys/cron/tables_to_merge"

/opt/cron/tools/chunk_size_distribution.py --create-merge-queue --queue-path $QUEUE --filter-out "//tmp" --filter-out "//sys"

/opt/cron/tools/run_parallel.sh "/opt/cron/tools/merge.py $QUEUE" 20 /opt/cron/merging_log

