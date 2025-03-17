LIBRARY()

SRCS(
    callback_context.h
    executor_impl.h
    executor_impl.cpp
    log_lazy.h
    retry_policy.cpp
    trace_lazy.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic

    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/types
    

    library/cpp/monlib/dynamic_counters
)

END()
