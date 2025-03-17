LIBRARY()

GENERATE_ENUM_SERIALIZATION(events.h)

GENERATE_ENUM_SERIALIZATION(event_ids.h)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/graph_params/proto
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/core/fq/libs/row_dispatcher/protos
    yql/essentials/core/facade
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/pq/proto
    yql/essentials/public/issue
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
