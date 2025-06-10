LIBRARY()

SRCS(
    constructor.cpp
    GLOBAL zero_level.cpp
    GLOBAL one_layer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
)

END()
