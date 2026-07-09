GTEST(unittester-flow-controller)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    async_balancer_ut.cpp
    compact_rebalance_actions_ut.cpp
    controller_ut.cpp
    job_manager_ut.cpp
    partitioning_ut.cpp
    resource_balancer_ut.cpp
    state_manager_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/controller/unittests/mock
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
