#!/bin/bash -eu

run_sort() {
    before=$(date +"%s")
    yt sort --src $1 --dst "$1_sorted" --sort-by key --spec "{pool=unfair}">>"sort$2" 2>&1
    after=$(date +"%s")
    diff=$(($after-$before))
    echo $diff >>"res$2"
}

run_op() {
    before=$(date +"%s")
    yt map "cat >/dev/null" --src $1 --dst "$1_output" --format dsv --spec "{pool=unfair}">>"op$2" 2>&1
    after=$(date +"%s")
    diff=$(($after-$before))
    echo $diff >>"res$2"
}


for RF in {1,3,5}; do
    echo "RF=$RF"
    echo "Initializing"
    rm -f "init$RF" "sort$RF" "res$RF"
    for i in {1..10}; do
        echo $i
        table="//home/tests/rf_experiment/$i"
        yt rm $table -f
        if [ "$i" = "1" ]; then
            yt create table $table
            yt set "$table/@replication_factor" $RF
            yt set "$table/@compression_codec" "zlib_9"
            yt map "cat" --src "//statbox/access-log/2012-12-01" --dst $table --format dsv --spec "{job_io={table_writer={upload_replication_factor=$RF}}}" >>"init$RF" 2>&1
        else
            yt copy "//home/tests/rf_experiment/1" "$table" >>"init$RF" 2>&1
        fi
    done;
    
    touch "res$RF"
    for i in {1..10}; do
        echo $i
        table="//home/tests/rf_experiment/$i"
        run_op $table $RF &
    done;

    while true; do
        lines=`cat "res$RF" | wc -l`
        if [ "$lines" = "10" ]; then
            echo "RESULT"
            cat "res$RF" | awk '{sum+=$1} END {print sum}'
            break
        fi
        sleep 1
    done
done;
