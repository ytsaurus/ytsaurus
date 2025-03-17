LIBRARY()

SRCS(
    index.cpp
    bucket.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
)

END()
