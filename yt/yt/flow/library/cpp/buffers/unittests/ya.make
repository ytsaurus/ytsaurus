GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    epoch_cycle_tracker_ut.cpp
    max_rate_estimator_ut.cpp
    offered_rate_estimator_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/buffers
)

SIZE(SMALL)

END()
