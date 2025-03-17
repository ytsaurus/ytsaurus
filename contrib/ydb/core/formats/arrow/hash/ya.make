LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/actors/protos
    contrib/ydb/library/formats/arrow/hash
    contrib/ydb/library/formats/arrow/common
    contrib/ydb/library/formats/arrow/simple_builder
)

SRCS(
    calcer.cpp
)

END()

