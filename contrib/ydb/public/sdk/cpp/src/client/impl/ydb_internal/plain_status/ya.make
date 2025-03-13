LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
