LIBRARY()

SRCS(
    grpc_service_v1.cpp
    grpc_service_v2.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/library/grpc/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/keyvalue
)

END()

RECURSE_FOR_TESTS(
    ut
)
