LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    library/cpp/monlib/metrics
)

END()
