LIBRARY(library-formats-arrow-accessor-common)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
)

SRCS(
    chunk_data.cpp
    const.cpp
)

END()
