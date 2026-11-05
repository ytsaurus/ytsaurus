GTEST(unittester-server-lib-chaos-election)

SRCS(
    chaos_lease_ut.cpp
    config_ut.cpp
    election_manager_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/cypress_election

    yt/yt/server/lib/chaos_election

    yt/yt/core/test_framework

    yt/yt/client/unittests/mock
)

END()
