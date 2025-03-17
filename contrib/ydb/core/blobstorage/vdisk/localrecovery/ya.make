LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/protos
)

SRCS(
    defs.h
    localrecovery_defs.cpp
    localrecovery_defs.h
    localrecovery_logreplay.cpp
    localrecovery_logreplay.h
    localrecovery_public.cpp
    localrecovery_public.h
    localrecovery_readbulksst.cpp
    localrecovery_readbulksst.h
)

END()
