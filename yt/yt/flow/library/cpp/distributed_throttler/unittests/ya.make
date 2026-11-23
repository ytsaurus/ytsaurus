GTEST(unittester-flow-distributed-throttler)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    factory_ut.cpp
    prefetching/bucket_ut.cpp
    prefetching/client_server_ut.cpp
    weighted_fair_queue_scheduler_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/distributed_throttler
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
