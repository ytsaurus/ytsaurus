G_BENCHMARK(table_client-benchmark)

OWNER(g:yt)

PEERDIR(
    yt/yt/experiments/random_row
    yt/yt/client/formats
    yt/yt/client
    yt/yt/library/skiff_ext
    yt/yt/core
    library/cpp/testing/benchmark
)

PROTO_NAMESPACE(yt)

SRCS(
    common_benchmarks.cpp
    schema_serialize.cpp
    skiff.cpp
    protobuf.cpp
    yson.cpp
    validate.cpp
    row.proto
)

FROM_SANDBOX(
    FILE 2035409933 OUT_NOAUTO schema.yson
)

RESOURCE(
    schema.yson schema.yson
)

END()

