PY3_PROGRAM(clickhouse-trampoline)

PY_MAIN(clickhouse-trampoline)

PY_SRCS(
    TOP_LEVEL
    clickhouse-trampoline.py
)

PEERDIR(
    library/python/svn_version
    contrib/python/requests
    yt/python/yt/yson
)

END()

