GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    proto_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/parsers
    yt/yt/flow/library/cpp/parsers/unittests/proto
)

SIZE(SMALL)

END()
