RECURSE(
    add_message_distribute_flag
    at_most_once_sink
    companion
    computation_cycles_and_buffers
    diagnostic_tools
    external_joiner
    flow_execute
    ipv4_support
    keep_order_mode
    keep_order_mode/pipeline
    key_visitor/cpp
    key_visitor/cpp/pipeline
    key_visitor/joiner
    key_visitor/joiner/pipeline
    key_visitor/python
    key_visitor/python/pipeline
    message_filter
    multi_cluster
    pipeline_alter
    process_function_overhead
    process_function_overhead/pipeline
    reanimate_vanilla
    reanimate_vanilla/cpp
    reanimate_vanilla/cpp/pipeline
    reanimate_vanilla/python
    reanimate_vanilla/python/pipeline
    secret_env
    secret_env/pipeline
    servicelog
    shuffle
    start_stop_pipeline_stress
    state_joiner
    static_table
    static_table_v2
    swift_map_batching
    swift_map_batching/pipeline
    table_injector
    test_distributed_throttler
    test_distributed_throttler/pipeline
    word_count_sync
    working_pipeline_telemetry
    working_pipeline_telemetry/pipeline
)

IF (NOT SANITIZER_TYPE)
    RECURSE(
        test_compact_output_store
        test_input_store
        test_timer_store
    )
ENDIF()

IF (NOT OPENSOURCE)
    # Uses replicated tables — out of yt_sync_mini's scope.
    RECURSE(
        sorted_dynamic_table
    )

    RECURSE(
        key_visitor/java
        key_visitor/java/companion
        key_visitor/java_external
        key_visitor/java_external/companion
        reanimate_vanilla/java
        reanimate_vanilla/java/pipeline
    )
ENDIF()
