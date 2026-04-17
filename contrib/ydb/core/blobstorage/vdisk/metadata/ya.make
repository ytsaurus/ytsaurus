LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/blobstorage/vdisk/common
)

SRCS(
    metadata_actor.cpp
    metadata_actor.h
)

END()
