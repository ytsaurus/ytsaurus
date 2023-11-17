UNION()

FROM_SANDBOX(
    5376409921
    OUT_NOAUTO
    spyt_cluster/conf/sidecar-config/livy-client.template.conf
    spyt_cluster/conf/sidecar-config/livy.template.conf
    spyt_cluster/conf/sidecar-config/metrics.properties
    spyt_cluster/conf/inner-sidecar-config/livy-client.template.conf
    spyt_cluster/conf/inner-sidecar-config/livy.template.conf
    spyt_cluster/conf/inner-sidecar-config/metrics.properties
    spyt_cluster/conf/inner-sidecar-config/solomon-agent.template.conf
    spyt_cluster/conf/inner-sidecar-config/solomon-service-master.template.conf
    spyt_cluster/conf/inner-sidecar-config/solomon-service-worker.template.conf
    spyt_cluster/conf/inner-sidecar-config/ytserver-proxy.template.yson
    spyt_cluster/spark-extra.zip
    spyt_cluster/spark-yt-data-source.jar
    spyt_cluster/spark-yt-launcher.jar
    spyt_cluster/spyt.zip
)

FROM_SANDBOX(
    5376408429
    RENAME spark_fork/spark.tgz
    OUT_NOAUTO spyt_cluster/spark.tgz
)

END()
