#!/bin/sh -eu

FROM=$1
TO=$2

YT_MAPREDUCE="/home/ignat/yt/scripts/python_wrapper/mapreduce"
MAPREDUCE="../mapreduce"

"$YT_MAPREDUCE" -map "./mapreduce -server n01-0449g.yt.yandex.net:8013 -write $TO -append" -src "$FROM" -dst "dev_null" -file "$MAPREDUCE"
