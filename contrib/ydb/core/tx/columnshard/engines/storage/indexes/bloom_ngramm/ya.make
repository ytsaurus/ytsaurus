LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
    const.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/portions
)

END()
