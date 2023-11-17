LIBRARY()

SRCS(
    background_controller.cpp
    blob.cpp
    blob_cache.cpp
    blob_manager.cpp
    columnshard__init.cpp
    columnshard__notify_tx_completion.cpp
    columnshard__plan_step.cpp
    columnshard__progress_tx.cpp
    columnshard__propose_cancel.cpp
    columnshard__propose_transaction.cpp
    columnshard__read.cpp
    columnshard__read_base.cpp
    columnshard__scan.cpp
    columnshard__index_scan.cpp
    columnshard__stats_scan.cpp
    columnshard__write.cpp
    columnshard__write_index.cpp
    columnshard.cpp
    columnshard_impl.cpp
    columnshard_common.cpp
    columnshard_private_events.cpp
    columnshard_schema.cpp
    counters.cpp
    defs.cpp
    read_actor.cpp
    write_actor.cpp
    tables_manager.cpp
    tx_controller.cpp
)

GENERATE_ENUM_SERIALIZATION(columnshard.h)
GENERATE_ENUM_SERIALIZATION(columnshard_impl.h)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/control
    contrib/ydb/core/formats
    contrib/ydb/core/kqp
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/engines
    contrib/ydb/core/tx/columnshard/engines/writer
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/operations
    contrib/ydb/core/tx/columnshard/blobs_reader
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/resource_subscriber
    contrib/ydb/core/tx/columnshard/normalizer/granule
    contrib/ydb/core/tx/columnshard/normalizer/portion
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/tracing
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/util
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/chunks_limiter
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
