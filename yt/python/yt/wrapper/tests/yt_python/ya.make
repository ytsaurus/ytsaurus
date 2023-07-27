OWNER(g:yt g:yt-python exprmntr)

PROGRAM(yt-python)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/python/yt
    yt/python/yt/cli
    yt/python/yt/yson
    yt/python/yt/clickhouse
    yt/python/yt/wrapper
    yt/python/yt/environment

    yt/yt/python/yt_yson_bindings
    yt/yt/python/yt_driver_rpc_bindings

    library/python/pymain
)

PY_SRCS(
    TOP_LEVEL
    __init__.py
)

PY_MAIN(library.python.pymain:run)

NO_CHECK_IMPORTS()

END()
