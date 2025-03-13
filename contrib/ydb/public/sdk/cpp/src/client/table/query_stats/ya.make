LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/query
)

END()
