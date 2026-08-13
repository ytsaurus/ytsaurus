GTEST(unittester-flow-vanilla)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    spec_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/vanilla
)

END()
