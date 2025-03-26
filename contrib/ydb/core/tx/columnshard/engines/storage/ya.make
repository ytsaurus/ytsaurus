LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer
    contrib/ydb/core/tx/columnshard/engines/storage/actualizer
    contrib/ydb/core/tx/columnshard/engines/storage/chunks
    contrib/ydb/core/tx/columnshard/engines/storage/indexes
    contrib/ydb/core/tx/columnshard/engines/storage/granule
    contrib/ydb/core/formats/arrow
)

END()
