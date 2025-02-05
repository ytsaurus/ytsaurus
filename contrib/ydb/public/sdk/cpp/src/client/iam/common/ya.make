LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    iam.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
)

END()
