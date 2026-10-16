GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    retryable_async_request_functions_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/retryable_async_request/lib
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
