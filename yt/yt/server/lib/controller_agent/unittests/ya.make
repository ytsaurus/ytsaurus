GTEST(unittester-server-lib-controller-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    job_statistics_wire_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/controller_agent

    yt/yt/core
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
