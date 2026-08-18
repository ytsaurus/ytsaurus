RECURSE(
    download_jfr
    flamegraph
    job_investigation
    pipeline_chaos_monkey
    python_companion_package
    reanimate_vanilla_operation
    reshard_flow_tables
    yt_sync_mini
)

IF (NOT OPENSOURCE)
    RECURSE(
        draw_pipeline_graph
    )
ENDIF()
