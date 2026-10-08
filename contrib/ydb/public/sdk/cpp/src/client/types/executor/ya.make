LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    executor.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/executor
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/thread_pool
    contrib/ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
