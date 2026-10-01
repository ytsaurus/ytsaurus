GTEST(unittester-chaos-cache)

SRCS(
    chaos_cache_service_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/server/lib/chaos_cache
    yt/yt/ytlib
    yt/yt/ytlib/test_framework
)

SIZE(SMALL)

END()
