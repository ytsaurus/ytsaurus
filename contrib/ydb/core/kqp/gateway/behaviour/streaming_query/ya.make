LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    initializer.cpp
    manager.cpp
    object.cpp
    optimization.cpp
    queries.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    library/cpp/retry
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/gateway/behaviour/streaming_query/common
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/protos
    contrib/ydb/core/protos/schemeshard
    contrib/ydb/core/resource_pools
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/conclusion
    contrib/ydb/library/query_actor
    contrib/ydb/library/table_creator
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/services/metadata
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
    contrib/ydb/services/metadata/optimization
    yql/essentials/core
    yql/essentials/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    common
)
