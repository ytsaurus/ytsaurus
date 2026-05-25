LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/client/metrics
    contrib/ydb/public/sdk/cpp/src/client/impl/observability/error_category
    library/cpp/monlib/metrics
)

END()
