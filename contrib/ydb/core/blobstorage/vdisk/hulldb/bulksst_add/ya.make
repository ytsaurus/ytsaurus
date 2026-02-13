LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/protos
)

SRCS(
    hulldb_bulksst_add.cpp
    hulldb_bulksst_add.h
    hulldb_fullsyncsst_add.h
)

END()

