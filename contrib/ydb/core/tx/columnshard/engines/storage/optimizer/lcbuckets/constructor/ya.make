LIBRARY()

SRCS(
    GLOBAL constructor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/selector
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/level
)

END()
