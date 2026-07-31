GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    distributing_tracker_ut.cpp
    flow_core_build_info_ut.cpp
    flow_core_version_ut.cpp
    flow_view_ut.cpp
    generate_message_id_ut.cpp
    inflight_tracker_ut.cpp
    interval_ut.cpp
    job_directory_ut.cpp
    key_builder_ut.cpp
    key_ut.cpp
    message_batch_ut.cpp
    message_batcher_ut.cpp
    message_ut.cpp
    payload_ut.cpp
    persisted_state_ut.cpp
    registry_ut.cpp
    resource_manager_ut.cpp
    schema_ut.cpp
    spec_ut.cpp
    state_ut.cpp
    state_cache_ut.cpp
    stream_inflight_limits_ut.cpp
    stream_spec_storage_ut.cpp
    time_provider_ut.cpp
    traverse_ut.cpp
    visit_ut.cpp
    yson_message_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/library/query/engine
)

SIZE(MEDIUM)

END()

RECURSE(
    binary_checksum_override
    mock
)
