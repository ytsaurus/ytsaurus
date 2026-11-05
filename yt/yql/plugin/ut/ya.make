GTEST(unittester-yql-plugin)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config_ut.cpp
)

PEERDIR(
    yt/yql/plugin
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
