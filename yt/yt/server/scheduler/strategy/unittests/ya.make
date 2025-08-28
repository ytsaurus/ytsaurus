GTEST(unittester-scheduler-strategy)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    gpu_allocation_assignment_plan_update_ut.cpp
    packing_ut.cpp
    pool_tree_element_ut.cpp
    scheduling_policy_ut.cpp
    operation_controller_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/scheduler/strategy

    yt/yt/server/lib/scheduler

    yt/yt/core
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
