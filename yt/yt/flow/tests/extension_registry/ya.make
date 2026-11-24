GTEST(unittester-flow-extension-registry)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    registry_ut.cpp
)

PEERDIR(
    yt/yt/flow/extensions/ydb/cpp
    yt/yt/core/test_framework
)

END()
