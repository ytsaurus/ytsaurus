LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/portions
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc
)

END()
