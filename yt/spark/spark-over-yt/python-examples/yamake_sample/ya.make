PY3_PROGRAM(smoke_test)

PEERDIR(
    yt/spark/spark-over-yt/data-source/src/main
)

PY_SRCS(
    MAIN main.py
)

END()
