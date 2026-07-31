LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    computation_base.cpp
    computation_tracer.cpp
    controller_base.cpp
    event_timestamp_assigner.cpp
    external_state_manager_base.cpp
    key_visitor.cpp
    static_table_key_visitor_joiner.cpp
    key_visitor_store.cpp
    message_filter.cpp
    meta_setter.cpp
    passthrough_computation.cpp
    simple_external_state_manager.cpp
    job_state/state_providers.cpp
    job_state/state_manager.cpp
    stores/compact_output_store.cpp
    stores/input_store.cpp
    stores/timer_store.cpp
    swift_map_computation.cpp
    swift_ordered_source_computation.cpp
    transform_computation.cpp
    universal_controller_helpers.cpp
    universal_controller.cpp
    watermark_aligner.cpp
    watermark_generator.cpp
    GLOBAL register.cpp
)

PEERDIR(
    library/cpp/containers/absl
    library/cpp/containers/insert_only_concurrent_cache
    library/cpp/iterator
    library/cpp/yt/misc
    yt/yt/flow/library/cpp/serializer
    yt/yt/library/query/base
    yt/yt/library/query/engine_api
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/distributed_throttler
    yt/yt/flow/library/cpp/resources
    yt/yt/flow/library/cpp/tables
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/misc
    yt/yt/library/profiling/sensors_owner
)

END()

RECURSE_FOR_TESTS(
    unittests
)
