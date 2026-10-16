GTEST(unittester-flow-balancer-simulation)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    scenarios_ut.cpp
    simulation.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/controller/unittests/mock
    yt/yt/core/test_framework
    library/cpp/testing/common
)

SIZE(MEDIUM)

FORK_SUBTESTS()

END()
