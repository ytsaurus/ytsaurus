LIBRARY()

SRCS(
    queries.cpp
    schema.cpp
)

PEERDIR(
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/actor/cfg
    contrib/ydb/core/ymq/actor/cloud_events
)

END()
