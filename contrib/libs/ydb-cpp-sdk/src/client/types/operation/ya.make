LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    operation.cpp
    out.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
    contrib/libs/ydb-cpp-sdk/src/client/types
)

END()
