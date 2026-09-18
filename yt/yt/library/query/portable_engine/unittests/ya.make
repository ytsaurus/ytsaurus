GTEST(unittester-library-query-portable-engine)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    registry_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/query/portable_engine
)

SIZE(SMALL)

END()
