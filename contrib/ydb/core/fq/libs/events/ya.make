LIBRARY()

GENERATE_ENUM_SERIALIZATION(events.h)

GENERATE_ENUM_SERIALIZATION(event_ids.h)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/fq/libs/graph_params/proto
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
