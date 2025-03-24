LIBRARY(library-formats-arrow-scalar)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow/switch
    contrib/ydb/library/actors/core
)

SRCS(
    serialization.cpp
)

END()
