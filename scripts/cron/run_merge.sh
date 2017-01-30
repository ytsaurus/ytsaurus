#!/bin/bash -eux

echo "Running merge script for cluster $YT_PROXY" >&2

QUEUE="//sys/cron/tables_to_merge"

/opt/cron/tools/find_tables_to_merge.py --queue-path $QUEUE --minimum-number-of-chunks 100 --filter-out //statbox --filter-out //home/qe --filter-out //sys

/opt/cron/tools/find_tables_to_merge.py --queue-path $QUEUE --minimum-number-of-chunks 100 --root //statbox --account statbox-logs --append

/opt/cron/tools/run_parallel.sh "/opt/cron/tools/merge.py $QUEUE" 40 "/opt/cron/merging_log_$YT_PROXY"

echo "Merge script for cluster $YT_PROXY finished" >&2

