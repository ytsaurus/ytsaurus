GTEST(unittester-queue-client)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    dynamic_state_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/ytlib
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
