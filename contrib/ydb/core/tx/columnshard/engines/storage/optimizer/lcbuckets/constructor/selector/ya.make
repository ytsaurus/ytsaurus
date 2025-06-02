LIBRARY()

SRCS(
    constructor.cpp
    GLOBAL empty.cpp
    GLOBAL transparent.cpp
    GLOBAL snapshot.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
)

END()
