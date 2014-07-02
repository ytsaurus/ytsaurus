#!/bin/bash -eux

QUEUE="//sys/cron/tables_to_merge"

/opt/cron/tools/chunk_size_distribution.py --create-merge-queue --queue-path $QUEUE --minimal-age 36 --filter-out "//sys" --filter-out "//tmp" --filter-out "//crypta/production/tmp" --filter-out "//crypta/production/user_events" --filter-out "//crypta/production/lal_manager/data_to_classify"

/opt/cron/tools/run_parallel.sh "/opt/cron/tools/merge.py $QUEUE" 40 "/opt/cron/merging_log_$YT_PROXY"

