#!/bin/bash -eux

[ -n "$LOG_NAME" ] || echo "You should specify LOG_NAME -- path to the file with log"
[ -n "$SYSTEM" ] || echo "You should specify SYSTEM name of log in the table"

line_number=1
#LOG_NAME="/yt/logs/scheduler-scheduler01-001g.debug.log"
table='<append=true>//home/ignat/logs'

export YT_PROXY="proxy.yt.yandex.net"
export YT_TOKEN="b3c50e75ab834aa9ac0eb7f0ec3404cb"

upload()
{
    yt2 create table "$table" --ignore-existing --recursive
    exec $@ | pv | ./format $SYSTEM | yt2 write "$table" --format dsv
}

while true; do
    line_count=$(cat ${LOG_NAME} | wc -l)
    if [ "$line_number" -lt "$((line_count + 1))" ]; then
        upload sed "${line_number},${line_count}"'!d' $LOG_NAME
    else
        old_line_count=$(wc -l "${LOG_NAME}.1")
        upload sed "${line_number},${old_line_count}"'!d' "${LOG_NAME}.1"
        upload sed "1,${line_count}"'!d' $LOG_NAME
    fi
    line_number=$((line_count + 1))

    sleep 60
done
