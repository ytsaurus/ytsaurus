LIBRARY()


SRCS(
    meta.cpp
    constructor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes/abstract
)

END()
