LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/grpc/client
    contrib/ydb/library/yql/public/issue
)

END()
