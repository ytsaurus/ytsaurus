G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    compact_unversioned_owning_row_bench.cpp
    cow_tree_bench.cpp
    indexed_yson_string_bench.cpp
    identifier_bench.cpp
    keyed_heap_bench.cpp
    prefetch_bench.cpp
)

PEERDIR(
    library/cpp/containers/absl
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/misc
)

SIZE(MEDIUM)

END()
