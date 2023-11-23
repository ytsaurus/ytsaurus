PY3_LIBRARY()

PEERDIR(
    build/platform/java/jdk/jdk11
    yt/python/yt/wrapper
    yt/spark/spark-over-yt/data-source/src/main
    yt/spark/spark-over-yt/tools/release
)

PY_SRCS(
    NAMESPACE spyt.testing.common
    __init__.py
    cluster.py
    cypress.py
    helpers.py
)

END()
