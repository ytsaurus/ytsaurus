GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    parse_executor_ut.cpp
    parse_function_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/examples/cpp/file_parsing/lib
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/library/query/engine
)

END()
