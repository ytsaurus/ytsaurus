LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    codec.cpp
    companion_main.cpp
    companion_service.cpp
    config.cpp
    job.cpp
    job_registry.cpp
    output_collector.cpp
    pipeline.cpp
    resource_store.cpp
    runtime_context.cpp
    runtime_init_context.cpp
    server.cpp
    state_store.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/library/program
)

END()

RECURSE_FOR_TESTS(
    unittest
)
