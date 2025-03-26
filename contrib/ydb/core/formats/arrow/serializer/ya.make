LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/services/metadata/abstract
    contrib/ydb/library/actors/core
    contrib/ydb/library/formats/arrow/common
    contrib/ydb/core/protos
)

SRCS(
    abstract.cpp
    GLOBAL native.cpp
    stream.cpp
    parsing.cpp
    utils.cpp
)

END()
