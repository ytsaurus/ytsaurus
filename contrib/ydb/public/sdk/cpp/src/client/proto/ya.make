LIBRARY()

SRCS(
    accessor.cpp
)

CFLAGS(
    GLOBAL -DYDB_SDK_INTERNAL_CLIENTS
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
