GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    availability_groups_ut.cpp
    compact_output_store_ut.cpp
    computation_base_ut.cpp
    computation_tracer_ut.cpp
    event_timestamp_assigner_ut.cpp
    idle_partition_ut.cpp
    input_store_ut.cpp
    static_table_key_visitor_joiner_ut.cpp
    key_visitor_store_ut.cpp
    key_visitor_ut.cpp
    late_data_partitions_ut.cpp
    message_filter_ut.cpp
    meta_setter_ut.cpp
    resource_ut.cpp
    state_client_ut.cpp
    swift_map_validation_ut.cpp
    timer_store_ut.cpp
    universal_controller_helpers_ut.cpp
    watermark_generator_ut.cpp
)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/tables/unittests/mock
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
