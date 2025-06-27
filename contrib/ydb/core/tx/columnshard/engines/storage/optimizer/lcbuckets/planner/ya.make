LIBRARY()

SRCS(
    GLOBAL optimizer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector
)

END()
