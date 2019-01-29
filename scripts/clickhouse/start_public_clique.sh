yt start-clickhouse-clique --enable-monitoring --instance-count 32 --cpu-limit 2 --enable-query-log --spec '{owners=[yandex]; title="Public testing clique"; max_failed_job_count=10000; pool="chyt"; alias="*ch_public"}' 

