GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    codec_ut.cpp
    config_ut.cpp
    job_registry_ut.cpp
    job_ut.cpp
    monitoring_ut.cpp
    output_collector_ut.cpp
    pipeline_ut.cpp
    process_batch_ut.cpp
    profiling_ut.cpp
    resource_store_ut.cpp
    runtime_context_ut.cpp
    server_ut.cpp
    state_store_ut.cpp
)

PEERDIR(
    library/cpp/json/yson
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion/server
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/resources
)

SIZE(SMALL)

END()
