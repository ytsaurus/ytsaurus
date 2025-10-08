LIBRARY()

SRCS(
    account_read_quoter.cpp
    autopartitioning_manager.cpp
    consumer_offset_tracker.cpp
    offload_actor.cpp
    ownerinfo.cpp
    partition.cpp
    partition_blob_encoder.cpp
    partition_compactification.cpp
    partition_compaction.cpp
    partition_init.cpp
    partition_monitoring.cpp
    partition_read.cpp
    partition_sourcemanager.cpp
    partition_write.cpp
    quota_tracker.cpp
    read_quoter.cpp
    sourceid.cpp
    subscriber.cpp
    user_info.cpp
    write_quoter.cpp
)



PEERDIR(
    contrib/ydb/core/backup/impl
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/persqueue/public/write_meta
    contrib/ydb/core/persqueue/pqtablet/blob
    contrib/ydb/core/persqueue/pqtablet/cache
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/persqueue/pqtablet/partition/mirrorer
)

END()

RECURSE(
    mirrorer
)

RECURSE_FOR_TESTS(
    ut
)
