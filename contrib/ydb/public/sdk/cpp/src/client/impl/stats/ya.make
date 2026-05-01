LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/client/metrics
    library/cpp/monlib/metrics
)

END()
