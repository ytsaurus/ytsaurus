LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    params.cpp
    impl.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/types/fatal_error_handlers
    contrib/libs/ydb-cpp-sdk/src/client/value
)

END()
