LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/core/grpc_services
    contrib/ydb/public/api/grpc
    contrib/ydb/library/actors/core
    contrib/ydb/library/grpc/server
)

END()
