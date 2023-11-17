LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/grpc/client
    contrib/ydb/library/yql/public/issue
)

END()
