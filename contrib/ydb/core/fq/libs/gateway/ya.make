LIBRARY()

SRCS(
    empty_gateway.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/read_rule
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/fq/libs/tasks_packer
    contrib/ydb/library/yql/providers/common/token_accessor/client
    yql/essentials/public/issue
    yql/essentials/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
