GTEST(unittester-server-lib-tablet_server)

ALLOCATOR(YT)

SRCS(
    replicated_table_tracker_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/client/unittests/mock
    yt/yt_proto/yt/client
    yt/yt/server/lib/tablet_server
)

SIZE(MEDIUM)

END()
