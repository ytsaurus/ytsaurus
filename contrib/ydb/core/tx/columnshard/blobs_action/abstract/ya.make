LIBRARY()

SRCS(
    gc.cpp
    common.cpp
    read.cpp
    write.cpp
    remove.cpp
    storage.cpp
    action.cpp
    storages_manager.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
)

END()
