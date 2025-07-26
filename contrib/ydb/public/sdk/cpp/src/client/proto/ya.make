LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
