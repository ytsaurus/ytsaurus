LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/tiling
)

END()

RECURSE_FOR_TESTS(
    ut
)
