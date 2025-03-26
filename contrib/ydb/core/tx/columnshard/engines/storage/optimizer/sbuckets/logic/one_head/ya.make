LIBRARY()

SRCS(
    logic.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract
    contrib/ydb/core/tx/columnshard/engines/changes
    contrib/ydb/core/tx/columnshard/common
)

END()
