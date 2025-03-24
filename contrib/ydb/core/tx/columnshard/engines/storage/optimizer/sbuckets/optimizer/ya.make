LIBRARY()

SRCS(
    GLOBAL optimizer.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/index
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
)

END()
