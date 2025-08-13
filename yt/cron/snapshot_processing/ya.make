RECURSE(
    export_master_snapshot
    validate_master_snapshot
)

IF (NOT OPENSOURCE)
    RECURSE(
        dump_master_snapshot
        table_statistics_monitor
        validate_operation_snapshot
        validate_tablet_cell_snapshot
    )
ENDIF()
