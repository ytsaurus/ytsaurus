PY3_PROGRAM(spyt_cli)

PEERDIR(
    yt/spark/spark-over-yt/data-source/src/main
)

# Change extension
COPY_FILE(yt/spark/spark-over-yt/data-source/src/main/bin/spark-discovery-yt spark-discovery-yt.py)
COPY_FILE(yt/spark/spark-over-yt/data-source/src/main/bin/spark-launch-yt spark-launch-yt.py)
COPY_FILE(yt/spark/spark-over-yt/data-source/src/main/bin/spark-manage-yt spark-manage-yt.py)
COPY_FILE(yt/spark/spark-over-yt/data-source/src/main/bin/spark-shell-yt spark-shell-yt.py)
COPY_FILE(yt/spark/spark-over-yt/data-source/src/main/bin/spark-submit-yt spark-submit-yt.py)

PY_SRCS(
    MAIN spyt_cli.py
    spark-discovery-yt.py
    spark-launch-yt.py
    spark-manage-yt.py
    spark-shell-yt.py
    spark-submit-yt.py
)

END()
