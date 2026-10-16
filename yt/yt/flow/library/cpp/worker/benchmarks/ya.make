G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    input_buffer_bench.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/worker
)

SIZE(MEDIUM)

END()
