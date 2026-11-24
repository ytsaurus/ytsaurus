LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/api/protos
)

END()
