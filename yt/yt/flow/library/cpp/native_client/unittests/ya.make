GTEST(unittester-flow-native-client)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    pipeline_init_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/native_client
)

SIZE(SMALL)

END()
