LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
