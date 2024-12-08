LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    chunk_list_pool.cpp
    config.cpp
    controller_agent.cpp
    controller_agent_service.cpp
    counter_manager.cpp
    helpers.cpp
    intermediate_chunk_scraper.cpp
    job_monitoring_index_manager.cpp
    job_prober_service.cpp
    job_profiler.cpp
    job_size_constraints.cpp
    job_tracker.cpp
    job_tracker_service.cpp
    master_connector.cpp
    memory_watchdog.cpp
    operation.cpp
    operation_controller.cpp
    operation_controller_host.cpp
    partitioning_parameters_evaluator.cpp
    private.cpp
    program.cpp
    scheduling_context.cpp
    snapshot_builder.cpp
    snapshot_downloader.cpp
    tentative_tree_eligibility.cpp
    virtual.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    yt/yt/library/ytprof
    yt/yt/library/server_program

    yt/yt/server/lib
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/scheduler
    yt/yt/server/lib/controller_agent

    yt/yt/server/controller_agent/controllers

    library/cpp/getopt

    contrib/libs/tcmalloc/malloc_extension
)

END()

RECURSE_FOR_TESTS(
    unittests
)
