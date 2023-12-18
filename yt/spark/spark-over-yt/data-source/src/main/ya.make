PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    yt/spark/spark-over-yt/data-source/src/main/python
    contrib/python/ytsaurus-pyspark
    contrib/python/pyarrow
    contrib/python/PyYAML
)

FROM_SANDBOX(
    5551064742 AUTOUPDATED spyt_client
    RENAME spyt_client/spark-yt-file-system.jar OUT_NOAUTO spark-extra/jars/spark-yt-file-system.jar
    RENAME spyt_client/spark-yt-submit.jar OUT_NOAUTO spyt/jars/spark-yt-submit.jar
)

RESOURCE_FILES(
    PREFIX yt/spark/spark-over-yt/data-source/src/main/
    spark-extra/bin/driver-op-discovery.sh
    spark-extra/bin/job-id-discovery.sh
    spark-extra/conf/log4j.clusterLog.properties
    spark-extra/conf/log4j.clusterLogJson.properties
    spark-extra/conf/log4j.properties
    spark-extra/conf/log4j.worker.properties
    spark-extra/conf/spark-defaults.conf
    spark-extra/conf/spark-env.sh
    spark-extra/jars/spark-yt-file-system.jar
    spyt/jars/spark-yt-submit.jar
)

END()

RECURSE(bin)
