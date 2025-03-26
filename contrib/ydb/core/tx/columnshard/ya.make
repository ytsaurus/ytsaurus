LIBRARY()

SRCS(
    background_controller.cpp
    blob.cpp
    blob_cache.cpp
    columnshard__init.cpp
    columnshard__notify_tx_completion.cpp
    columnshard__plan_step.cpp
    columnshard__progress_tx.cpp
    columnshard__propose_cancel.cpp
    columnshard__propose_transaction.cpp
    columnshard__scan.cpp
    columnshard__statistics.cpp
    columnshard_subdomain_path_id.cpp
    columnshard__write.cpp
    columnshard__write_index.cpp
    columnshard.cpp
    columnshard_impl.cpp
    columnshard_private_events.cpp
    columnshard_schema.cpp
    columnshard_view.cpp
    counters.cpp
    defs.cpp
    inflight_request_tracker.cpp
    write_actor.cpp
    tables_manager.cpp
)

GENERATE_ENUM_SERIALIZATION(columnshard.h)
GENERATE_ENUM_SERIALIZATION(columnshard_impl.h)

PEERDIR(
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/control/lib
    contrib/ydb/core/formats
    contrib/ydb/core/kqp
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/blobs_action/storages_manager
    contrib/ydb/core/tx/columnshard/blobs_reader
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/data_accessor
    contrib/ydb/core/tx/columnshard/data_accessor/in_mem
    contrib/ydb/core/tx/columnshard/data_locks
    contrib/ydb/core/tx/columnshard/data_sharing
    contrib/ydb/core/tx/columnshard/engines
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/tx/columnshard/engines/writer
    contrib/ydb/core/tx/columnshard/export
    contrib/ydb/core/tx/columnshard/loading
    contrib/ydb/core/tx/columnshard/normalizer
    contrib/ydb/core/tx/columnshard/operations
    contrib/ydb/core/tx/columnshard/resource_subscriber
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/subscriber
    contrib/ydb/core/tx/columnshard/tablet
    contrib/ydb/core/tx/columnshard/transactions
    contrib/ydb/core/tx/columnshard/transactions/operators
    contrib/ydb/core/tx/columnshard/tx_reader
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/tx/priorities/service
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/time_cast
    contrib/ydb/core/tx/tracing
    contrib/ydb/core/util
    contrib/ydb/library/actors/core
    contrib/ydb/library/chunks_limiter
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/public/api/protos
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    engines
    splitter
)

RECURSE_FOR_TESTS(
    ut_rw
    ut_schema
)
