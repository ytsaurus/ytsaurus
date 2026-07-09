GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    key_visitor_functions_ut.cpp
)

PEERDIR(
    yt/yt/flow/tests/key_visitor/cpp/lib
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
