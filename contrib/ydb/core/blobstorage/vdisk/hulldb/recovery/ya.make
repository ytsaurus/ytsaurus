LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    contrib/ydb/core/protos
)

SRCS(
    hulldb_recovery.cpp
    hulldb_recovery.h
)

END()

