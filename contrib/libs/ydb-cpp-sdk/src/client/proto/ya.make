LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    accessor.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/params
    contrib/libs/ydb-cpp-sdk/src/client/value
)

END()
