RECURSE(
    actors
    clique
    common
    common/attrs
    control
    gateway
    global_worker_manager
    interface
    local_gateway
    metrics
    provider
    runtime
    scheduler
    service
    stats_collector
    task_runner
    task_runner_actor
    testlib
    worker_manager
)

IF (NOT OPENSOURCE AND NOT OS_WINDOWS)
    RECURSE(
        clique_discovery
    )
ENDIF()
