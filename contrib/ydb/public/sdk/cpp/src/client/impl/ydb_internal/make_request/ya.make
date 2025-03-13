LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/api/protos
)

END()
