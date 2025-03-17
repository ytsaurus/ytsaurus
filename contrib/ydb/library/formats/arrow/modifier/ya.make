LIBRARY(library-formats-arrow-modifier)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow/switch
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/library/actors/core
)

SRCS(
    schema.cpp
    subset.cpp
)

END()
