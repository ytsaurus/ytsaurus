GTEST(unittester-scheduler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    fair_share_tree_element_ut.cpp
    gpu_allocation_assignment_plan_update_ut.cpp
    helpers_ut.cpp
    job_metrics_ut.cpp
    packing_ut.cpp
    scheduling_policy_ut.cpp
    strategy_operation_controller_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/scheduler
)

SIZE(MEDIUM)

END()
