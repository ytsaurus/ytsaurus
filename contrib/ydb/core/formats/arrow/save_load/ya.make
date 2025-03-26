LIBRARY()

SRCS(
    saver.cpp
    loader.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/libs/apache/arrow
    contrib/ydb/library/accessor
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow/transformer
    contrib/ydb/library/formats/arrow/common
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/serializer
)

END()
