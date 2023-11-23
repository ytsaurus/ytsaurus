GTEST(cell-balancer-unittests)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bundle_scheduler_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/cell_balancer
)

SIZE(SMALL)

END()
