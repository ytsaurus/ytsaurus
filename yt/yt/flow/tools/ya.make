RECURSE(
    download_jfr
    flamegraph
    job_investigation
    pipeline_chaos_monkey
    python_companion_package
    reanimate_vanilla_operation
    reshard_flow_tables
    ui_test_controller
)

IF (NOT OPENSOURCE)
    RECURSE(
        draw_pipeline_graph
    )
ENDIF()
