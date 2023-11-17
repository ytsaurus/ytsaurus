LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/intervals
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/levels
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
)

END()

RECURSE_FOR_TESTS(
    ut
)
