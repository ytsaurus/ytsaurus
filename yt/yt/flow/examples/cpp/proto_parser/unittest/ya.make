GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    proto_parser_function_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/proto_parser/lib
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/library/query/engine
)

END()
