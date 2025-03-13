LIBRARY()

SRCS(
    query_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/threading/future
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/local_rpc
    yql/essentials/public/issue
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
