RECURSE(
    check_runner
    map_result
    master_chunk_management
    quorum_health
    register_watcher
    controller_agent_alerts
    scheduler_alerts
    sort_result
    suspicious_jobs
    tablet_cell_helpers
    tablet_cells
)
    

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()
