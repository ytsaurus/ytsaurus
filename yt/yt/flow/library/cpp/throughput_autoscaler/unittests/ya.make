GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    demand_ut.cpp
    steady_demand_history_ut.cpp
    feedback_policy_ut.cpp
    computation_warmup_ut.cpp
    processing_meter_ut.cpp
    processing_measurement_ut.cpp
    production_feedback_ut.cpp
    processing_observation_window_ut.cpp
    throughput_autoscaler_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/throughput_autoscaler
)

SIZE(SMALL)

END()
