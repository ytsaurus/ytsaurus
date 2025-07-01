PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/python/yt/environment/arcadia_interop
    yt/python/yt/environment/components/query_tracker
    yt/python/yt/environment/components/yql_agent
    yt/python/yt/local
    yt/python/yt/yson
)

END()
