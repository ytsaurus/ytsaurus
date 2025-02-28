LIBRARY(library-formats-arrow-switch)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/formats/arrow/validation
)

SRCS(
    switch_type.cpp
    compare.cpp
)

END()
