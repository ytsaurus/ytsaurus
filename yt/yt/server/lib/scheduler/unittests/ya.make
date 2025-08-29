GTEST(unittester-server-lib-scheduler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    job_metrics_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/scheduler

    yt/yt/core
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
