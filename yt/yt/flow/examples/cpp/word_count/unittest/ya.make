GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    word_count_functions_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/word_count/lib
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
