LIBRARY()

SRCS(
    iam.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
)

END()
