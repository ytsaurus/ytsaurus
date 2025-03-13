LIBRARY()

SRCS(
    logic.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/formats/arrow
)

END()
