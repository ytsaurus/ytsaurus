G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    payload_converter_bench.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
    yt/yt/library/query/engine_api
    yt/yt/client
    yt/yt/core
)

SIZE(MEDIUM)

END()
