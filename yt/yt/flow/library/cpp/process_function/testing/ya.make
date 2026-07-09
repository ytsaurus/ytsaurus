LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    entity_builders.cpp
    in_memory_external_state_manager.cpp
    process_function_test_harness.cpp
    recording_output_collector.cpp
    test_runtime_context.cpp
    test_state_environment.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/tables/unittests/mock
    yt/yt/library/query/engine
)

END()
