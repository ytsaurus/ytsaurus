LIBRARY()

SRCS(
    cloud_events.h
    cloud_events.cpp
)

PEERDIR(
    contrib/ydb/core/ymq/actor/cloud_events/proto
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
    contrib/ydb/core/util
    contrib/ydb/core/ymq/base
    contrib/ydb/core/audit
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    cloud_events_ut
)
