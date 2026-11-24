LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    callback_context.h
    executor_impl.h
    executor_impl.cpp
    log_lazy.h
    retry_policy.cpp
    trace_lazy.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic

    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/types
    

    library/cpp/monlib/dynamic_counters
)

END()
