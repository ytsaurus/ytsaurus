LIBRARY()

SRCS(
    defs.h
    vdisk_actor.cpp
    vdisk_actor.h
    vdisk_services.h
)

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/anubis_osiris
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/defrag
    contrib/ydb/core/blobstorage/vdisk/huge
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/blobstorage/vdisk/hullop
    contrib/ydb/core/blobstorage/vdisk/ingress
    contrib/ydb/core/blobstorage/vdisk/localrecovery
    contrib/ydb/core/blobstorage/vdisk/protos
    contrib/ydb/core/blobstorage/vdisk/query
    contrib/ydb/core/blobstorage/vdisk/repl
    contrib/ydb/core/blobstorage/vdisk/scrub
    contrib/ydb/core/blobstorage/vdisk/skeleton
    contrib/ydb/core/blobstorage/vdisk/syncer
    contrib/ydb/core/blobstorage/vdisk/synclog
)

END()

RECURSE(
    anubis_osiris
    balance
    common
    defrag
    huge
    hulldb
    hullop
    ingress
    localrecovery
    query
    repl
    scrub
    skeleton
    syncer
    synclog
)
