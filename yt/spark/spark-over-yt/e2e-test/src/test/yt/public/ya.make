PY3_LIBRARY()

PEERDIR(
    yt/python/client
    yt/spark/spark-over-yt/e2e-test/src/test/yt/common
)

PY_SRCS(
    NAMESPACE spyt.testing.public
    base.py
)

END()
