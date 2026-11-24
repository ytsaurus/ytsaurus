LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/plain_status
    contrib/libs/ydb-cpp-sdk/src/client/types
    contrib/libs/ydb-cpp-sdk/src/client/types/fatal_error_handlers
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()
