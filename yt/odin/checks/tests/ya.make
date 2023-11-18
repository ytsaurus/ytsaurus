PY3TEST()

TEST_SRCS(
    conftest.py
    test_check_runner.py
    test_configuration.py
    test_controller_agent_alerts.py
    test_controller_agent_count.py
    test_controller_agent_uptime.py
    test_controller_agent_operation_memory_consumption.py
    test_destroyed_replicas_size.py
    test_discovery.py
    test_dynamic_table_commands.py
    test_lost_vital_chunks.py
    test_map_result.py
    test_master_alerts.py
    test_master_chunk_management.py
    test_oauth_health.py
    test_quorum_health.py
    test_scheduler_uptime.py
    test_sort_result.py
    test_tablet_cells.py
    test_tablet_cell_snapshots.py
    test_unaware_nodes.py
)

PEERDIR(
    contrib/python/mock
    yt/odin/lib/yt_odin/test_helpers
    yt/odin/lib/yt_odin/logserver
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/chyt_clique_liveness
    yt/odin/checks/lib/master_chunk_management
    yt/odin/checks/lib/quorum_health
    yt/odin/checks/lib/system_quotas
    yt/odin/checks/lib/tablet_cells
    yt/odin/checks/lib/tablet_cell_helpers
    yt/python/yt/environment/arcadia_interop
    yt/python/yt/environment
)

DATA(
    arcadia/yt/odin/checks/config
    arcadia/yt/odin/checks/data
)

DEPENDS(
    yt/odin/checks/bin/controller_agent_alerts
    yt/odin/checks/bin/controller_agent_count
    yt/odin/checks/bin/controller_agent_uptime
    yt/odin/checks/bin/controller_agent_operation_memory_consumption
    yt/odin/checks/bin/destroyed_replicas_size
    yt/odin/checks/bin/discovery
    yt/odin/checks/bin/dynamic_table_commands
    yt/odin/checks/bin/lost_vital_chunks
    yt/odin/checks/bin/map_result
    yt/odin/checks/bin/master_alerts
    yt/odin/checks/bin/master_chunk_management
    yt/odin/checks/bin/oauth_health
    yt/odin/checks/bin/quorum_health
    yt/odin/checks/bin/scheduler_uptime
    yt/odin/checks/bin/sort_result
    yt/odin/checks/bin/system_quotas
    yt/odin/checks/bin/tablet_cell_snapshots
    yt/odin/checks/bin/tablet_cells
    yt/odin/checks/bin/unaware_nodes
    yt/odin/checks/config
    yt/odin/bin/yt_odin
    yt/yt/packages/tests_package
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

FORK_SUBTESTS()

REQUIREMENTS(
    ram_disk:4
    cpu:4
    ram:32
)

SIZE(MEDIUM)

TAG(
    ya:full_logs
    ya:huge_logs
)

END()
