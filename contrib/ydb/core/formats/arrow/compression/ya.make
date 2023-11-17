LIBRARY()

SRCS(
    diff.cpp
    object.cpp
    parsing.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/conclusion
)

END()
