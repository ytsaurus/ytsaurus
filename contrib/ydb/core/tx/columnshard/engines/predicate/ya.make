LIBRARY()

SRCS(
    container.cpp
    range.cpp
    filter.cpp
    predicate.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
)

END()
