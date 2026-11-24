LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()
