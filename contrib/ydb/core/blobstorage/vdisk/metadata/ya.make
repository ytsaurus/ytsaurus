LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/blobstorage/vdisk/common
)

SRCS(
    metadata_actor.cpp
    metadata_actor.h
    metadata_context.h
)

END()
