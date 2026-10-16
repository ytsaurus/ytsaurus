LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    GLOBAL proto_parser_function.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/proto_parser/proto
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/parsers
    yt/yt/flow/library/cpp/process_function
)

END()
