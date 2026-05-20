LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
    const.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow/hash
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/portions
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/helper
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    contrib/ydb/library/conclusion
)

END()
