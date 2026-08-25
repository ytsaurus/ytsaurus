GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    registry_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/random
)

SIZE(SMALL)

END()
