LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    buffer_state_manager.cpp
    config.cpp
    controller_connector.cpp
    input_buffer.cpp
    input_buffer_detail.cpp
    input_manager.cpp
    job_spec.cpp
    job_tracker.cpp
    job.cpp
    message_distributor.cpp
    message_distributor_detail.cpp
    message_service.cpp
    traced_invoker.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/yt/memory
    library/cpp/yt/string

    yt/yt/library/heavy_hitters
    yt/yt/library/profiling/sensors_owner
    yt/yt/library/query/engine
    yt/yt/library/ytprof
    yt/yt/core
    yt/yt/client
    yt/yt/flow/lib/client

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/worker
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/distributed_throttler
    yt/yt/flow/library/cpp/misc

    contrib/libs/tcmalloc/malloc_extension
)

END()

RECURSE_FOR_TESTS(
    benchmarks
    unittests
)
