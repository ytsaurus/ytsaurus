RECURSE(
    actors
    clique
    common
    control
    gateway
    global_worker_manager
    local_gateway
    metrics
    provider
    runtime
    scheduler
    service
    stats_collector
)

IF (NOT OPENSOURCE AND NOT OS_WINDOWS)
    RECURSE(
        clique_discovery
    )
ENDIF()
