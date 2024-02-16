PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/python/yson/arrow
    yt/yt/python/yt_driver_rpc_bindings
    yt/yt/python/yt_yson_bindings
    yt/python/yt/clickhouse
    yt/python/yt/wrapper
)

END()
