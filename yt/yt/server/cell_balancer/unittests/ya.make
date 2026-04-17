GTEST(cell-balancer-unittests)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    allocation_from_spare_ut.cpp
    bundle_scheduler_ut.cpp
    chaos_scheduler_ut.cpp
    helpers.cpp
    pod_id_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/cell_balancer
)

SIZE(SMALL)

END()
