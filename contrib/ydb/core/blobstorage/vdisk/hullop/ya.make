LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/hulldb/barriers
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/blobstorage/vdisk/hulldb/cache_block
    contrib/ydb/core/blobstorage/vdisk/hulldb/recovery
    contrib/ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    contrib/ydb/core/blobstorage/vdisk/hulldb/compstrat
    contrib/ydb/core/blobstorage/vdisk/hullop/hullcompdelete
    contrib/ydb/core/blobstorage/vdisk/synclog
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_buildslice.h
    blobstorage_hull.cpp
    blobstorage_hull.h
    blobstorage_hullactor.h
    blobstorage_hullactor.cpp
    blobstorage_hullcommit.h
    blobstorage_hullcompactdeferredqueue.h
    blobstorage_hullcompact.h
    blobstorage_hullcompactworker.h
    blobstorage_hullload.h
    blobstorage_hulllog.cpp
    blobstorage_hulllog.h
    blobstorage_hulllogcutternotify.cpp
    blobstorage_hulllogcutternotify.h
    blobstorage_readbatch.h
    defs.h
    hullop_compactfreshappendix.cpp
    hullop_compactfreshappendix.h
    hullop_delayedresp.h
    hullop_entryserialize.cpp
    hullop_entryserialize.h
)

END()

RECURSE(
    hullcompdelete
)

RECURSE_FOR_TESTS(
    ut
)
