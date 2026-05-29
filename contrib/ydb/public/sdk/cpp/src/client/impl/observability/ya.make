LIBRARY()

SRCS(
    metric_buffer.cpp
    metrics.cpp
    observation.cpp
    span.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/metrics
    contrib/ydb/public/sdk/cpp/src/client/trace
    contrib/ydb/public/sdk/cpp/src/client/impl/stats
    contrib/ydb/public/sdk/cpp/src/client/impl/observability/error_category
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state
)

END()
