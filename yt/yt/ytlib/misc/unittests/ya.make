GTEST(unittester-ytlib-memory-trackers)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    memory_reference_tracker_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/ytlib
)

SIZE(SMALL)

END()
