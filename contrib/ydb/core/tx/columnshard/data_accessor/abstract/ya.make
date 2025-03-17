LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/protos
)

END()
