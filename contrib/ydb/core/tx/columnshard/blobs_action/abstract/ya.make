LIBRARY()

SRCS(
    gc.cpp
    gc_actor.cpp
    common.cpp
    blob_set.cpp
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
    contrib/ydb/core/tx/tiering/abstract
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/common
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/blobs_action/events
    contrib/ydb/core/tx/columnshard/blobs_action/protos
)

END()
