G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    process_batch_bench.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/flow/library/cpp/companion/server
    yt/yt/flow/library/cpp/process_function/testing
)

SIZE(MEDIUM)

END()
