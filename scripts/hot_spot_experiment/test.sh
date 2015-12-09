#!/bin/bash -eu

run_sort() {
    before=$(date +"%s")
    yt2 sort --src $1 --dst "$1_sorted" --sort-by key --spec "{pool=unfair}">>"sort$2" 2>&1
    after=$(date +"%s")
    diff=$(($after-$before))
    echo $diff >>"res$2"

}


for RF in {3,5}; do
    echo "RF=$RF"
    echo "Initializing"
    rm -f "init$RF" "sort$RF" "res$RF"
    for i in {1..10}; do
        echo $i
        table="//home/tests/rf_experiment/$i"
        yt2 rm $table -f
        yt2 create table $table
        yt2 set "$table/@replication_factor" $RF
        yt2 map "cat" --src //home/tests/synth/terasort/input_1000GB_compressed_x3 --dst $table --format dsv --spec "{job_io={table_writer={upload_replication_factor=$RF}}}" >>"init$RF" 2>&1
    done;
    
    touch "res$RF"
    for i in {1..10}; do
        echo $i
        table="//home/tests/rf_experiment/$i"
        run_sort $table $RF &
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
