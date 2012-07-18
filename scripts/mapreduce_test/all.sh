#!/bin/sh -eu

export PYTHONPATH="/home/ignat/yt/scripts/python_wrapper"

# MAPREDUCE
#export SYSTEM="mapreduce"
#export SERVER="n01-0449g.yt.yandex.net:8013"
#export MAPREDUCE="./mapreduce"

# Octo testing
export SYSTEM="yt"
export SERVER="w301.hdp.yandex.net"
export MAPREDUCE="/home/ignat/yt/scripts/python_wrapper/mapreduce"

# YT testing
#export SYSTEM="yt"
#export SERVER="proxy.yt.yandex.net"
#export MAPREDUCE="/home/ignat/yt/scripts/python_wrapper/mapreduce"

# YT development
#export SYSTEM="yt"
#export SERVER="n01-0650g.yt.yandex.net:80"
#export MAPREDUCE="/home/ignat/yt/scripts/python_wrapper/mapreduce"

#export PARAMS="-jobcount 1000 -opt cpu.intensive.mode=1"
export PARAMS=""

for (( iter = 1 ; iter <= 1; iter++ ))
do
    echo "Iteration $iter"

    for START in 10000; do
        export START
        export INPUT="speed_test/input$START"
        export OUTPUT="speed_test/output$START"

        echo "Gen operation"
        time ./prepare.sh

        echo "Sort operation"
        time ./sort.sh
    done
    #echo "Merge sorted tables"
    #time ./merge.sh "speed_test/output10000" "speed_test/output20000" "speed_test/merged"

    #echo "Statistic task"
    #export PATH=.:$PATH
    #cd job && ./job.ymr && cd ..
done

