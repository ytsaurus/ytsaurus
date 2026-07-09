RECURSE(
    download_jfr
    flamegraph
    job_investigation
    pipeline_chaos_monkey
    reanimate_vanilla_operation
    reshard_flow_tables
)

IF (NOT OPENSOURCE)
    RECURSE(
        draw_pipeline_graph
        generate_yson_struct_doc
    )
ENDIF()
