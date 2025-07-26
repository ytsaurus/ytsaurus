LIBRARY()

SRCS(
    access_service.h
)

PEERDIR(
    contrib/ydb/public/api/client/nc_private/iam/v1
    contrib/ydb/library/actors/core
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/core/base
)

END()
