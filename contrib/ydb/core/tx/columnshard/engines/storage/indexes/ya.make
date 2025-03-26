LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/portions
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/bloom
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/bits_storage
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/categories_bloom
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/max
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
)

END()
