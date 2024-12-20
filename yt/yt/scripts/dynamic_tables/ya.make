RECURSE(
    build_secondary_index
)

IF (NOT OPENSOURCE)
    RECURSE(
        add_table_replica
        check_tablet_cell_snapshot_convergence
        gradual_remount
        migrate_dynamic_tables
        remount_old_tablets
        run_dyntable_remote_copy
        suspend_resume_tablet_cells
        tablet_balancer_dry_run_snapshot_maker
        zone_supervisor
        bundle_hotfix
    )
ENDIF()
