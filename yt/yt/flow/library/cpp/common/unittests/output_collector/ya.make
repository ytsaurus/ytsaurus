GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    output_collector_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
)

SIZE(SMALL)

END()
