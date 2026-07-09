GTEST(unittester-flow-distributed-throttler)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    bucket_ut.cpp
    client_server_ut.cpp
    factory_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/distributed_throttler
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
