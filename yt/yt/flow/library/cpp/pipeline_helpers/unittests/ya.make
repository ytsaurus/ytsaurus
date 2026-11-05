GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    pipeline_ut.cpp
)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/pipeline_helpers
)

SIZE(SMALL)

END()
