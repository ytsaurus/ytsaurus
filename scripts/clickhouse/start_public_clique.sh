if [[ "$1" == "" ]] ; then
    BIN="//sys/clickhouse/bin/ytserver-clickhouse"
else
    BIN="$1"
fi

echo "Using binary $BIN"

yt start-clickhouse-clique \
    --enable-monitoring \
    --instance-count 16 \
    --cpu-limit 8 \
    --enable-query-log \
    --spec "
    {
        acl=[{
            subjects=[yandex];
            permissions=[read];
            action=allow;
        }];
        title=\"Public testing clique\";
        max_failed_job_count=10000;
        pool=\"chyt\";
        alias=\"*ch_public\"
     }" \
    --cypress-geodata-path //sys/clickhouse/geodata/geodata.tgz \
    --cypress-ytserver-clickhouse-path $BIN
