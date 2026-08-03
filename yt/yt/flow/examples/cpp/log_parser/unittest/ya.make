GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    log_line_parser_ut.cpp
    log_parser_process_function_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/log_parser/lib
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/library/query/engine
)

END()
