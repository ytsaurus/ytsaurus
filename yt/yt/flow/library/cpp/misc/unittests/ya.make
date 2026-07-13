GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    identifier_ut.cpp
    compact_unversioned_owning_row_ut.cpp
    cow_tree_ut.cpp
    debug_build_warning_ut.cpp
    linear_system_ut.cpp
    crash_recorder_ut.cpp
    bipartite_map_ut.cpp
    ema_ut.cpp
    error_backtrace_enricher_ut.cpp
    indexed_yson_string_ut.cpp
    keyed_heap_ut.cpp
    lexicographically_serialize_ut.cpp
    mutable_unversioned_row_ut.cpp
    ordered_memory_ut.cpp
    prefetch_ut.cpp
    retryable_client_ut.cpp
    retryable_transaction_ut.cpp
    status_profiler_ut.cpp
    weighted_random_ut.cpp
    remedian_splitter_ut.cpp
)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/misc
)

SIZE(SMALL)

END()
