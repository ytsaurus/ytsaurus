G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(shard_routing_bench.cpp)

PEERDIR(
    yt/yt/flow/extensions/clickhouse/cpp
)

SIZE(MEDIUM)

END()
