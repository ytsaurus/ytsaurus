#!/bin/bash -eux

DATE=$1
CLUSTER=$2

echo "$CLUSTER: $DATE"

export YT_PROXY="$CLUSTER.yt.yandex.net"

./yt/fennel/binaries/revert.py //statbox/kafka-import-prestable/yt-scheduler-log/$DATE //home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-after-logbroker $CLUSTER
./yt/fennel/binaries/remove_microseconds.py --input //sys/scheduler/event_log --output //home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-before-logbroker --date $DATE

python -c "import yt.wrapper as yt; yt.run_sort('//home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-before-logbroker', sort_by=['timestamp', 'job_id', 'event_type', 'operation_id']);"

python -c "import yt.wrapper as yt; yt.run_sort('//home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-after-logbroker', sort_by=['timestamp', 'job_id', 'event_type', 'operation_id']);"


./yt/fennel/binaries/diff.py //home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-before-logbroker //home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-after-logbroker //home/tramsmm/yt-scheduler-log-$CLUSTER-$DATE-diff
