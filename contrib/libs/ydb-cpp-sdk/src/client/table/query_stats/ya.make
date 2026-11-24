LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/query
)

END()
