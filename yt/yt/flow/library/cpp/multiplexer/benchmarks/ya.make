G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    multiplexer_process_function_bench.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/multiplexer
    yt/yt/flow/library/cpp/process_function/testing
)

SIZE(MEDIUM)

END()
