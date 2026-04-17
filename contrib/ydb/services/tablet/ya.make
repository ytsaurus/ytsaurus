LIBRARY()

SRCS(
    ydb_tablet.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services/tablet
)

END()
