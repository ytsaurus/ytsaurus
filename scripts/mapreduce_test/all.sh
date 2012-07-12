#!/bin/sh -eu

export SERVER="n01-0449g.yt.yandex.net:8013"
#export PARAMS="-jobcount 1000 -opt cpu.intensive.mode=1"
export PARAMS=""
rm -f err

for (( iter = 1 ; iter <= 5; iter++ ))
do
    echo "Iteration $iter"

    for START in 10000 20000; do
        export START
        export INPUT="speed_test/input$START"
        export OUTPUT="speed_test/output$START"

        echo "Upload data"
        time ./prepare.sh 2>>err

        echo "Map operation"
        time ./prepare_map.sh 2>>err

        echo "Sort operation"
        time ./sort.sh  -opt cpu.intensive.mode=1 2>>err
    done
    echo "Merge sorted tables"
    time ./merge.sh "speed_test/output10000" "speed_test/output20000" "speed_test/merged" 2>>err

    echo "Statistic task"
    export PATH=.:$PATH
    cd job && ./job.ymr
done

