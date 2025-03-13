LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
    contrib/ydb/library/formats/arrow/transformer
    contrib/ydb/library/formats/arrow/common
    contrib/ydb/library/formats/arrow/simple_builder
)

SRCS(
    conversion.cpp
    object.cpp
    diff.cpp
)

END()
