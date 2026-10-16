GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    registry_ut.cpp
    source_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/common/unittests/mock
)

SIZE(SMALL)

END()
