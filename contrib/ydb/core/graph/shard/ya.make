LIBRARY()

SRCS(
    backends.cpp
    backends.h
    log.h
    schema.h
    shard_impl.cpp
    shard_impl.h
    tx_aggregate_data.cpp
    tx_change_backend.cpp
    tx_get_metrics.cpp
    tx_init_schema.cpp
    tx_monitoring.cpp
    tx_startup.cpp
    tx_store_metrics.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/graph/api
    contrib/ydb/core/graph/shard/protos
)

END()

RECURSE_FOR_TESTS(ut)
