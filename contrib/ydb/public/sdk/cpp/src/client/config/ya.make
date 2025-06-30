LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
