GTEST(unittester-tablet-balancer-lib)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    balancing_helpers_ut.cpp
    bounded_priority_queue_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/tablet_balancer
    yt/yt/server/lib/tablet_balancer/dry_run/lib
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
