LIBRARY(library-formats-arrow-splitter)

SRCS(
    stats.cpp
    similar_packer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
)

GENERATE_ENUM_SERIALIZATION(stats.h)

END()
