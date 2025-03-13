LIBRARY()

SRCS(
    optimizer.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/hooks/abstract
)

END()
