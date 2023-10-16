PY3_PROGRAM(mapreduce-yt)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/clickhouse
    yt/yt/python/yt_yson_bindings
)

COPY_FILE(yt/python/yt/wrapper/bin/mapreduce-yt __main__.py)

PY_SRCS(__main__.py)

END()
