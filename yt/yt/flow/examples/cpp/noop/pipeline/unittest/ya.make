GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    noop_functions_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/noop/pipeline/lib
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
