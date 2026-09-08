GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    partitioning_helpers_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/partitioning
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
