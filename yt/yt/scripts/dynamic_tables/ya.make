RECURSE(
    build_secondary_index
    bundle_controller_tools
)

IF (NOT OPENSOURCE)
    RECURSE(
        add_table_replica
        check_tablet_cell_snapshot_convergence
        i_want_to_dryrun_tamed_cell
        gradual_remount
        migrate_dynamic_tables
        remount_old_tablets
        run_dyntable_remote_copy
        suspend_resume_tablet_cells
        tablet_balancer_dry_run_snapshot_maker
        zone_supervisor
        bundle_hotfix
        sequoia_bundle_configuration
    )
ENDIF()
