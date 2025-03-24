LIBRARY()

PEERDIR(
    contrib/ydb/core/tablet
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/core/base
    contrib/ydb/core/tx/data_events/common
    contrib/ydb/core/tx/sharding
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/library/accessor
    contrib/ydb/library/conclusion
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/wilson

    # Temporary fix dep ydb/core/tx/columnshard
    contrib/ydb/core/tablet_flat/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/blobstorage/vdisk/protos
    #
)

SRCS(
    shard_writer.cpp
    shards_splitter.cpp
    write_data.cpp
    columnshard_splitter.cpp
)

END()
