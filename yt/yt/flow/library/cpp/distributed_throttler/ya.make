LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    bucket.cpp
    client.cpp
    config.cpp
    factory.cpp
    metrics_throttler.cpp
    remote_throttler.cpp
    server.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/distributed_throttler/proto
    yt/yt/flow/library/cpp/misc
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(unittests)
