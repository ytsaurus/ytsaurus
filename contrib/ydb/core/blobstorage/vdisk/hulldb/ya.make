LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb/barriers
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/hulldb/compstrat
    contrib/ydb/core/blobstorage/vdisk/hulldb/fresh
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/blobstorage/vdisk/hulldb/recovery
    contrib/ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_hullgcmap.h
    hull_ds_all.h
    hull_ds_all_snap.h
)

END()

RECURSE(
    barriers
    base
    bulksst_add
    cache_block
    compstrat
    fresh
    generic
    recovery
    test
)
