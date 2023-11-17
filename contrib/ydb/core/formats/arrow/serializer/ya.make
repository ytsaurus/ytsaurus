LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/common
    library/cpp/actors/core
    contrib/ydb/core/protos
)

SRCS(
    abstract.cpp
    full.cpp
    batch_only.cpp
    stream.cpp
)

END()
