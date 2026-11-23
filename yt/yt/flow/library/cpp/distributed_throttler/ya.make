LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    config.cpp
    factory.cpp
    local_throttler.cpp
    prefetching/bucket.cpp
    prefetching/client.cpp
    prefetching/config.cpp
    prefetching/remote_throttler.cpp
    server.cpp
    weighted_fair_queue_scheduler.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/distributed_throttler/proto
    yt/yt/flow/library/cpp/misc
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(unittests)
