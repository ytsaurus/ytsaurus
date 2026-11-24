LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    executor.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/executor
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/thread_pool
    contrib/libs/ydb-cpp-sdk/src/client/types/exceptions
)

END()
