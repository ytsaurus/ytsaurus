#!/bin/bash -eux

QUEUE="//sys/cron/tables_to_merge"

/opt/cron/tools/find_tables_to_merge.py --queue-path $QUEUE --minimal-age 36

/opt/cron/tools/run_parallel.sh "/opt/cron/tools/merge.py $QUEUE" 40 "/opt/cron/merging_log_$YT_PROXY"

