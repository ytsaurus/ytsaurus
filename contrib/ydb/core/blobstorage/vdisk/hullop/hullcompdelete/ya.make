LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_hullcompdelete.cpp
    blobstorage_hullcompdelete.h
)

END()
