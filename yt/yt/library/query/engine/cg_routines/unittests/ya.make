GTEST(unittester-library-query-engine-cg-routines)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    inferrum_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/query/engine
    yt/yt/library/query/engine_api
    yt/yt/client
)

SIZE(SMALL)

END()
