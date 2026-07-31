LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    yql/essentials/providers/common/proto
    yt/yql/providers/dq/config
)

SRCS(
    dq_scheduler.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
