LIBRARY()

SRCS(
    read_rule_creator.cpp
    read_rule_deleter.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/topic
    yql/essentials/providers/common/proto
)

YQL_LAST_ABI_VERSION()

END()
