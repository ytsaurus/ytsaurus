GTEST(unittester-scheduler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    helpers_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/scheduler

    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
