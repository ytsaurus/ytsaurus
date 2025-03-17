LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/library/formats/arrow/transformer
)

SRCS(
    dictionary.cpp
)

END()
