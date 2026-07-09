LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    backtrace_ypath_service.cpp
    compact_unversioned_owning_row.cpp
    counter.cpp
    debug_build_warning.cpp
    identifier.cpp
    cow_tree.cpp
    crash_recorder.cpp
    GLOBAL error_backtrace_enricher.cpp
    indexed_yson_string.cpp
    lexicographically_serialize.cpp
    linear_system.cpp
    load_throughput_throttler.cpp
    mutable_unversioned_row.cpp
    node_info.cpp
    ordered_memory.cpp
    reconfigurable.cpp
    retryable_client_spec.cpp
    retryable_client.cpp
    retryable_transaction.cpp
    status_profiler.cpp
    version_helpers.cpp
    weighted_random.cpp
)

PEERDIR(
    contrib/libs/eigen
    library/cpp/build_info
    library/cpp/yt/error
    library/cpp/yt/memory
    library/cpp/yt/misc
    yt/yt/client
    yt/yt/core
    yt/yt/flow/library/cpp/misc/proto
    yt/yt/flow/lib/client
    yt/yt/library/backtrace_introspector
)

END()

RECURSE(
    proto
)

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
    )
ENDIF()

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    RECURSE_FOR_TESTS(
        fuzz
    )
ENDIF()
