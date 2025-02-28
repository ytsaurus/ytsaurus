LIBRARY(library-formats-arrow-hash)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/formats/arrow/simple_builder
    contrib/ydb/library/formats/arrow/switch
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/actors/protos
)

SRCS(
    xx_hash.cpp
)

END()

