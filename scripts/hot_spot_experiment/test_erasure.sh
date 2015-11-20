#!/bin/bash -eu

export YT_PROXY=smith.yt.yandex.net

SUFFIX="_erasure"

run_sort() {
    before=$(date +"%s")
    yt2 map cat --src $1 --dst "${1}_mapped" --format "dsv" --spec "{pool=unfair;job_count=4000}">>"sort$2" 2>&1
    after=$(date +"%s")
    diff=$(($after-$before))
    echo $diff >>"res$2"
}

touch "res$SUFFIX"
for i in {1..10}; do
    echo $i
    table="//home/tests/copies/$i"
    run_sort $table $SUFFIX &
done;

while true; do
    lines=`cat "res$SUFFIX" | wc -l`
    if [ "$lines" = "10" ]; then
        echo "RESULT"
        cat "res$SUFFIX" | awk '{sum+=$1} END {print sum}'
        break
    fi
    sleep 1
done
