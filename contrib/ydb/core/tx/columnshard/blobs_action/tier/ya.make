LIBRARY()

SRCS(
    adapter.cpp
    gc.cpp
    gc_actor.cpp
    gc_info.cpp
    write.cpp
    read.cpp
    storage.cpp
    remove.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
)

END()
