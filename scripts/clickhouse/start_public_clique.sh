yt start-clickhouse-clique \
    --enable-monitoring \
    --instance-count 16 \
    --cpu-limit 8 \
    --enable-query-log \
    --spec '{owners=[yandex]; title="Public testing clique"; max_failed_job_count=10000; pool="chyt"; alias="*ch_public"}' \
    --cypress-geodata-path //sys/clickhouse/geodata/geodata.tgz
