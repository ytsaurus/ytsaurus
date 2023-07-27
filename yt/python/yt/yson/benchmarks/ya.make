G_BENCHMARK()

SIZE(MEDIUM)

SRCS(
    yson_benchmark.cpp
)

DATA(
    ext:data/yt_bench_scan.yson
    ext:data/skiff_bench.yson
    ext:data/event_log_small.yson
)

PEERDIR(
    yt/python/client
    library/cpp/testing/common
)

END()
