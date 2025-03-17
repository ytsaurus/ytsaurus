LIBRARY()

SRCS(
    info.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract

    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/common

    contrib/libs/apache/arrow
)

END()
