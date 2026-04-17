LIBRARY()

SRCS(
    autopartitioning_manager.cpp
    consumer_offset_tracker.cpp
    message_id_deduplicator.cpp
    offload_actor.cpp
    ownerinfo.cpp
    partition.cpp
    partition_blob_encoder.cpp
    partition_compactification.cpp
    partition_compaction.cpp
    partition_init.cpp
    partition_mlp.cpp
    partition_monitoring.cpp
    partition_read.cpp
    partition_sourcemanager.cpp
    partition_write.cpp
    sourceid.cpp
    subscriber.cpp
    user_info.cpp
)



PEERDIR(
    library/cpp/containers/absl_flat_hash
    contrib/ydb/core/backup/impl
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/persqueue/public/write_meta
    contrib/ydb/core/persqueue/pqtablet/blob
    contrib/ydb/core/persqueue/pqtablet/cache
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/persqueue/pqtablet/partition/mirrorer
    contrib/ydb/core/persqueue/pqtablet/partition/mlp
    contrib/ydb/core/persqueue/pqtablet/quota
)

END()

RECURSE(
    mirrorer
    mlp
)

RECURSE_FOR_TESTS(
    ut
)
