LIBRARY()

SRCS(
    simple.cpp
    scheme_info.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow/splitter
    contrib/ydb/library/formats/arrow/common
    contrib/ydb/core/formats/arrow/serializer
)

END()
