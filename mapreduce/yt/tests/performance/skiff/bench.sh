TABLE_PATH="//home/levysotsky/benchmarks/logs_afisha-access-log_1d_2018-02-03"

if [ "$1" == "dump" ]; then
    NUM_ROWS=$2
    yt --proxy freud read $TABLE_PATH'[:#'$NUM_ROWS']' --format='<format=binary>yson' > dump.yson
    ./dump/dump $TABLE_PATH $NUM_ROWS dump.skiff skiff-schema.yson
fi

echo "====================== Skiff ======================="
time ./skiff dump.skiff skiff skiff-schema.yson

echo "====================== YSON ========================"
time ./skiff dump.yson yson
