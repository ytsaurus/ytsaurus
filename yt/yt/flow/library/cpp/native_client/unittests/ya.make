GTEST(unittester-flow-native-client)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    pipeline_init_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/native_client
    # Test-only: the lock table schema is compared against its owner, which the library itself must
    # not depend on (it is reachable from ytlib).
    yt/yt/server/lib/chaos_election
)

SIZE(SMALL)

END()
