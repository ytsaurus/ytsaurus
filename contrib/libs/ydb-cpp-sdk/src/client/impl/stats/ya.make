LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
