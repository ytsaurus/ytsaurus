LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/lwtrace
    contrib/ydb/core/grpc_services
    contrib/ydb/core/protos
    contrib/ydb/library/login
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

END()
