GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    process_function_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
