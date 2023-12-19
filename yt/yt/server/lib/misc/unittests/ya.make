GTEST(unittester-server-lib-misc)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    max_min_balancer_ut.cpp
    release_queue_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/server/lib/misc
)

SIZE(MEDIUM)

END()
