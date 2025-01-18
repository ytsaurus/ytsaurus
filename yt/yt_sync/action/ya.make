PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    alter_replica_attributes.py
    alter_rtt_options.py
    alter_table_attributes.py
    alter_table_schema.py
    base.py
    clean_temporary_objects.py
    clone_chaos_replica.py
    clone_replica.py
    copy_replication_progress.py
    create_ordered_from.py
    create_replica.py
    create_replicated_table.py
    create_table.py
    eliminate_chunk_views.py
    force_compaction.py
    freeze_table.py
    generate_initial_replication_progress.py
    gradual_remount.py
    helpers.py
    mount_table.py
    move_table.py
    read_replication_progress.py
    read_total_row_count.py
    register_queue_export.py
    remote_copy.py
    remount_table.py
    remove_replica.py
    remove_table.py
    reshard_table.py
    set_replication_progress.py
    set_upstream_replica.py
    sleep.py
    switch_chaos_collocation_replicas.py
    switch_replica_mode.py
    switch_replica_state.py
    switch_rtt.py
    transform_table_schema.py
    unfreeze_table.py
    unmount_table.py
    wait_chaos_replication_lag.py
    wait_in_memory_preload.py
    wait_replicas_flushed.py
    wait_replicas_in_sync.py
)

PEERDIR(
    yt/yt_sync/core

    yt/python/client
)

END()

RECURSE_FOR_TESTS(
    ut
)
