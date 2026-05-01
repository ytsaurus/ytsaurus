LIBRARY()

SRCS(
    metrics.cpp
    observation.cpp
    span.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/metrics
    contrib/ydb/public/sdk/cpp/src/client/impl/stats
)

END()
