PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    yt/spark/spark-over-yt/spyt-package/src/main/python
    contrib/python/ytsaurus-pyspark
    contrib/python/pyarrow
    contrib/python/PyYAML
)

FROM_SANDBOX(
    5951526627 AUTOUPDATED spyt_cluster
    OUT_NOAUTO spyt_cluster/spyt-package.zip
)

RESOURCE_FILES(
    PREFIX yt/spark/spark-over-yt/spyt-package/src/main/
    spyt_cluster/spyt-package.zip
)

END()

RECURSE(bin)
