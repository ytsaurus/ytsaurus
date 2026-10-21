LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    demand.cpp
    steady_demand_history.cpp
    feedback_policy.cpp
    feedback_state.cpp
    computation_warmup.cpp
    processing_measurement.cpp
    processing_meter.cpp
    processing_observation_window.cpp
    throughput_autoscaler.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
