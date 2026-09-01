G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(SYSTEM)

SRCS(
    companion_batch_benchmark.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion
    yt/yt/library/query/engine
)

SIZE(MEDIUM)

END()
