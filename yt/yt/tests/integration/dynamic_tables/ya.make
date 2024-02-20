PY3_LIBRARY()

# For opensource.
PY_SRCS(
    __init__.py
)

TEST_SRCS(
    test_aggregate_columns.py
    test_ally_replicas.py
    test_background_activity_orchid.py
    test_backups.py
    test_bulk_insert.py
    test_cell_balancer.py
    test_bundle_controller.py
    test_chaos.py
    test_compaction_partitioning.py
    test_cross_cluster_transactions.py
    test_dynamic_tables.py
    test_dynamic_tables_acl.py
    test_dynamic_tables_hunks.py
    test_dynamic_tables_leases.py
    test_dynamic_tables_metadata.py
    test_dynamic_tables_profiling.py
    test_dynamic_tables_resource_limits.py
    test_dynamic_tables_state_transitions.py
    test_explain.py
    test_hunk_storage.py
    test_journals.py
    test_lookup.py
    test_map_reduce_over_dyntables.py
    test_mount_config.py
    test_ordered_dynamic_tables.py
    test_query.py
    test_read_dynamic_store.py
    test_read_dynamic_table.py
    test_replicated_dynamic_tables.py
    test_replicated_dynamic_tables_profiling.py
    test_secondary_index.py
    test_sorted_dynamic_tables.py
    test_sorted_dynamic_tables_master.py
    test_table_collocation.py
    test_tablet_actions.py
    test_tablet_balancer.py
    test_tablet_transactions.py
)

IF (NOT OPENSOURCE)
    SRCS(
        test_query_webassembly.py
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    bin
)
