LIBRARY()


SRCS(
    meta.cpp
    constructor.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/tx/columnshard/engines/storage/chunks
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    contrib/ydb/core/tx/columnshard/engines/portions
)

YQL_LAST_ABI_VERSION()

END()
