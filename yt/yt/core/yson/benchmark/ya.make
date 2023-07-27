G_BENCHMARK(yson-benchmark)

OWNER(g:yt)

PEERDIR(
    yt/yt/core
    library/cpp/resource
)

SRCS(
    protobuf_interop.cpp
    pull_parser.cpp

    proto/sample.proto
)

FROM_SANDBOX(
    FILE 2147399681 OUT_NOAUTO event_log_small.yson
)

RESOURCE(
    event_log_small.yson event_log_small.yson
)

END()

