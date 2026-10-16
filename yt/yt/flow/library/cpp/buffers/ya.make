LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    epoch_cycle_tracker.cpp
    max_rate_estimator.cpp
    offered_rate_estimator.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE(
    unittests
)
