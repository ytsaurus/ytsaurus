LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/blobstorage/vdisk/common
)

SRCS(
    chunk_keeper_actor.cpp
    chunk_keeper_data.cpp
    chunk_keeper_events.cpp
)

END()
