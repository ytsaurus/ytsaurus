LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    block_builder.cpp
    describe_traits.cpp
    shard.cpp
    sink.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    contrib/libs/clickhouse-cpp
    library/cpp/yt/farmhash
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/common
)

END()

RECURSE(benchmarks)

RECURSE_FOR_TESTS(unittests)
