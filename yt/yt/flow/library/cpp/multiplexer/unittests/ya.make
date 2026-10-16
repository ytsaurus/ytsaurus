GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    multiplexer_process_function_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/multiplexer
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/client/unittests/mock
)

END()
