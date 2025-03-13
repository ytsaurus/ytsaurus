LIBRARY()

SRCS(
    address.cpp
    gc.cpp
    gc_actor.cpp
    write.cpp
    read.cpp
    storage.cpp
    remove.cpp
    blob_manager.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
)

END()
