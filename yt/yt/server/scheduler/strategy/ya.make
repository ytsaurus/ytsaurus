LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    field_filter.cpp
    helpers.cpp
    job_resources_helpers.cpp
    operation.cpp
    operation_controller.cpp
    operation_state.cpp
    persistent_state.cpp
    pool_tree.cpp
    pool_tree_element.cpp
    pool_tree_profile_manager.cpp
    pool_tree_snapshot.cpp
    pools_config_parser.cpp
    resource_tree.cpp
    resource_tree_element.cpp
    scheduling_heartbeat_context.cpp
    scheduling_heartbeat_context_detail.cpp
    strategy.cpp

    # TODO(eshcherbin): Remove cyclic dependencies and extract policy to a separate build target.
    policy/gpu/assignment_plan_context_detail.cpp
    policy/gpu/assignment_plan_update.cpp
    policy/gpu/scheduling_policy.cpp
    policy/gpu/scheduling_policy_detail.cpp
    policy/gpu/structs.cpp
    policy/gpu/helpers.cpp
    policy/gpu/persistent_state.cpp

    policy/helpers.cpp
    policy/operation_shared_state.cpp
    policy/packing.cpp
    policy/packing_detail.cpp
    policy/persistent_state.cpp
    policy/pool_tree_snapshot_state.cpp
    policy/scheduling_heartbeat_context.cpp
    policy/scheduling_policy.cpp
    policy/scheduling_policy_detail.cpp
    policy/scheduling_segment_manager.cpp
    policy/structs.cpp
)

PEERDIR(
    yt/yt/server/scheduler/common

    yt/yt/server/lib/scheduler
    yt/yt/server/lib/controller_agent

    yt/yt/ytlib

    yt/yt/library/vector_hdrf

    yt/yt/core

    library/cpp/yt/threading
)

END()

RECURSE_FOR_TESTS(
    unittests
)
