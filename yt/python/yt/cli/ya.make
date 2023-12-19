PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/clickhouse
    yt/yt/python/yt_yson_bindings

    # It is required to run job shell.
    contrib/python/tornado/tornado-4
)

PY_SRCS(
    NAMESPACE yt.cli

    __init__.py
    strawberry_parser.py
    yt_binary.py
)

END()
