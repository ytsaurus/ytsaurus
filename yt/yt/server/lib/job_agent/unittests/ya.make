GTEST(unittested-job-agent-lib)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

ALLOCATOR(YT)

SRCS(
    gpu_info_provider_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/restrict_licenses_tests.inc)

PEERDIR(
    yt/yt/server/lib/job_agent
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
