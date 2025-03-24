LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow
    contrib/ydb/core/formats/arrow/splitter
)

SRCS(
    container.cpp
    adapter.cpp
)

END()
