LIBRARY()

SRCS(
    processor.h
    processor.cpp
    processor_impl.h
    processor_impl.cpp
    schema.h
    schema.cpp
    db_counters.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_configure.cpp
    tx_collect.cpp
    tx_aggregate.cpp
    tx_interval_summary.cpp
    tx_interval_metrics.cpp
    tx_top_partitions.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/grpc_services/counters
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

END()
