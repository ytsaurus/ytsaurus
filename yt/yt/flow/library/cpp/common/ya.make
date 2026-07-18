LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    column_evaluator_cache.cpp
    authenticator.cpp
    checksum.cpp
    computation_controller.cpp
    computation.cpp
    describe_traits.cpp
    distributing_tracker.cpp
    external_state_manager.cpp
    flow_core_build_info.cpp
    flow_core_version.cpp
    flow_view.cpp
    inflight_tracker.cpp
    input_context.cpp
    job_directory.cpp
    key.cpp
    lag.cpp
    message_batch.cpp
    message_batcher.cpp
    message_migration.cpp
    message.cpp
    payload_converter.cpp
    payload_validation.cpp
    payload.cpp
    process_function.cpp
    public.cpp
    registry.cpp
    resource_manager.cpp
    schema.cpp
    select_literals.cpp
    time_provider.cpp
    sink.cpp
    source_controller.cpp
    source.cpp
    spec.cpp
    state_cache.cpp
    state.cpp
    stream_inflight_limits.cpp
    stream_spec_storage_state.cpp
    stream_spec_storage.cpp
    timestamp_statistics.cpp
    timer.cpp
    traverse.cpp
    visit.cpp
    yson_message.cpp
    control_table.cpp
    yt_connector.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/client
    yt/yt/flow/library/cpp/serializer
    yt/yt/flow/library/cpp/common/proto
    yt/yt/flow/library/cpp/common/controller/proto
    yt/yt/flow/library/cpp/common/worker/proto
    yt/yt/flow/library/cpp/misc
    yt/yt/core
    yt/yt/library/signature/common
    yt/yt/library/signature/validation
    yt/yt_proto/yt/flow/controller/proto
    yt/yt/client
    yt/yt/client/cache
    yt/yt/library/auth
    yt/yt/library/formats
    yt/yt/library/heavy_schema_validation
    yt/yt/library/query/engine_api
    yt/yt/library/tvm/service
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/iterator
    library/cpp/containers/insert_only_concurrent_cache
    library/cpp/string_utils/levenshtein_diff
)

IF (OPENSOURCE)
    SRCS(internal_urls_opensource.cpp)
ELSE()
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
    )
ENDIF()
