GTEST(unittester-server-lib-exec-node)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    helpers_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/exec_node

    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
