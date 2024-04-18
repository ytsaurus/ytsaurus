PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(
        yt/yt/python/yson/arrow
        yt/yt/python/yt_driver_rpc_bindings
        yt/yt/python/yt_yson_bindings
        yt/python_py2/yt/clickhouse
        yt/python_py2/yt/wrapper
    )
ELSE()
    PEERDIR(
        yt/yt/python/yson/arrow
        yt/yt/python/yt_driver_rpc_bindings
        yt/yt/python/yt_yson_bindings
        yt/python/yt/clickhouse
        yt/python/yt/wrapper
    )
ENDIF()

END()
