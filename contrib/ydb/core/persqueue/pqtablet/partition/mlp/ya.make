LIBRARY()

SRCS(
    mlp_common.cpp
    mlp_consumer.cpp
    mlp_consumer_app.cpp
    mlp_consumer_metrics.cpp
    mlp_dlq_mover.cpp
    mlp_message_enricher.cpp
    mlp_storage.cpp
    mlp_storage__serialization.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/common/proxy
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/persqueue/public/write_meta
)

GENERATE_ENUM_SERIALIZATION(mlp_storage.h)

END()

RECURSE_FOR_TESTS(
    ut
)
