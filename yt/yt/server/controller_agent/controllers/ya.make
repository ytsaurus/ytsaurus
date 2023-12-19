LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    aggregated_job_statistics.cpp
    alert_manager.cpp
    auto_merge_director.cpp
    auto_merge_task.cpp
    chunk_pool_adapters.cpp
    competitive_job_manager.cpp
    data_balancer.cpp
    data_flow_graph.cpp
    experiment_job_manager.cpp
    extended_job_resources.cpp
    job_info.cpp
    job_helpers.cpp
    job_memory.cpp
    job_splitter.cpp
    helpers.cpp
    operation_controller_detail.cpp
    ordered_controller.cpp
    probing_job_manager.cpp
    speculative_job_manager.cpp
    sort_controller.cpp
    sorted_controller.cpp
    table.cpp
    task.cpp
    unordered_controller.cpp
    vanilla_controller.cpp
)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/server/lib
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/tablet_server
    yt/yt/library/safe_assert
)

END()

