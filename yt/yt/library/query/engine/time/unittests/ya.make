GTEST(unittester-library-query-engine-time)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    timestamp_floor_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/query/engine/time
)

SIZE(SMALL)

END()
