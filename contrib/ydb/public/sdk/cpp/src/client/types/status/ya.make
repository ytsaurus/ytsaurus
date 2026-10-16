LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
