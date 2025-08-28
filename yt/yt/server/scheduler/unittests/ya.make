GTEST(unittester-scheduler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    gpu_allocation_assignment_plan_update_ut.cpp
    helpers_ut.cpp
    job_metrics_ut.cpp
    packing_ut.cpp
    pool_tree_element_ut.cpp
    scheduling_policy_ut.cpp
    operation_controller_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/scheduler
)

SIZE(MEDIUM)

END()
