#!/bin/sh -eu

export PYTHONPATH="/home/ignat/yt/python/yt_wrapper:$PYTHONPATH"

# Octo cluster
HOSTS=85
THREADCOUNT=12

# -- YT --
export SYSTEM="yt"
export SERVER="w301.hdp.yandex.net"

# -- Mapreduce --
#export SYSTEM="mapreduce"
#export SERVER="w301.hdp.yandex.net:8013"

# YT testing cluster
#HOSTS=250
#THREADCOUNT=16

# -- Mapreduce --
#export SYSTEM="mapreduce"
#export SERVER="n01-0449g.yt.yandex.net:8013"

# -- YT --
#export SYSTEM="yt"
#export SERVER="proxy.yt.yandex.net"

# YT development cluster
#export SYSTEM="yt"
#export SERVER="n01-0650g.yt.yandex.net:80"

export JOBCOUNT=`echo "$HOSTS * $THREADCOUNT" | bc`
if [ "$SYSTEM" = "yt" ]; then
    export MAPREDUCE="/home/ignat/yt/python/yt_wrapper/mapreduce -server $SERVER -jobcount $JOBCOUNT -threadcount $THREADCOUNT"
else
    export MAPREDUCE="/home/ignat/yt/scripts/mapreduce_test/mapreduce -server $SERVER -jobcount $JOBCOUNT"
fi

for (( iter = 1 ; iter <= 1; iter++ ))
do
    #echo "Iteration $iter"

    #export START=10000
    #export INPUT="speed_test/input$START"
    #export OUTPUT="speed_test/output$START"

    #echo "Gen operation"
    #time ./prepare.sh

    #echo "Sort operation"
    #time ./sort.sh
    #for START in 10000; do
    #done

    #echo "Merge sorted tables"
    #time ./merge.sh "speed_test/output10000" "speed_test/output20000" "speed_test/merged"

    #echo "Statistic task"
    #./run_statistic.sh

    echo "Word count and mutual information"
    ./run_mutual_information.sh
done

