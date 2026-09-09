GTEST(unittester-server-lib-chaos-election)

SRCS(
    chaos_lease_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/chaos_election

    yt/yt/core/test_framework

    yt/yt/client/unittests/mock
)

END()
