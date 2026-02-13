LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/kesus/tablet
    contrib/ydb/public/api/grpc
    contrib/ydb/services/ydb
)

END()

RECURSE_FOR_TESTS(
    ut
)
