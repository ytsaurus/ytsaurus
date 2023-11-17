LIBRARY()

SRCS(
    query_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/threading/future
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
