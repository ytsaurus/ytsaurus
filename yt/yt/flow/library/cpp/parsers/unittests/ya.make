GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    proto_ut.cpp
    proto_process_function_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/parsers
    yt/yt/flow/library/cpp/parsers/unittests/proto
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
