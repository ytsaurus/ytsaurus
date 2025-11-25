LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    contrib/ydb/public/sdk/cpp/src/library/time
)

END()
