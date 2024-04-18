PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(
        yt/python_py2/yt/clickhouse
        yt/python_py2/yt/wrapper
        yt/yt/python/yt_yson_bindings
    )
ELSE()
    PEERDIR(
        yt/python/yt/clickhouse
        yt/python/yt/wrapper
        yt/yt/python/yt_yson_bindings
    )
ENDIF()

END()
