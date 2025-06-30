LIBRARY()

SRCS(
    view.cpp
    view_v0.cpp
    collection.cpp
)

PEERDIR(
    contrib/ydb/library/conclusion
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/core/formats/arrow/reader
)

END()
