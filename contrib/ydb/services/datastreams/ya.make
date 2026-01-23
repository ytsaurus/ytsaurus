LIBRARY()

SRCS(
    datastreams_proxy.cpp
    grpc_service.cpp
    next_token.cpp
    put_records_actor.cpp
    shard_iterator.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/library/cloud_permissions
    contrib/ydb/core/base
    contrib/ydb/core/client/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind
    contrib/ydb/core/persqueue/public/list_topics
    contrib/ydb/core/persqueue/public/partition_key_range
    contrib/ydb/core/persqueue/public/write_meta
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/services/datastreams/codes
    contrib/ydb/services/lib/actors
    contrib/ydb/services/lib/sharding
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/ydb
)

END()

RECURSE_FOR_TESTS(
    ut
)
