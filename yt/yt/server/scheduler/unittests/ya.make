GTEST(unittester-scheduler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    fair_share_packing_ut.cpp
    fair_share_strategy_operation_controller_ut.cpp
    fair_share_tree_element_ut.cpp
    fair_share_tree_job_scheduler_ut.cpp
    job_metrics_ut.cpp
    scheduler_helpers_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/server/scheduler
)

SIZE(MEDIUM)

END()
