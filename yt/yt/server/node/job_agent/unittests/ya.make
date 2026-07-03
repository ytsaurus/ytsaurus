GTEST(unittester-job-resource-manager)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    job_resource_manager_ut.cpp
)

PEERDIR(
    yt/yt/server/node

    yt/yt/core/test_framework
)

TAG(
    ya:yt
)

SIZE(SMALL)

END()
