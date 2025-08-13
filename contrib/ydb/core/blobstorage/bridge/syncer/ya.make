LIBRARY()
    SRCS(
        defs.h
        syncer.cpp
        syncer.h
    )

    PEERDIR(
        contrib/ydb/core/base
        contrib/ydb/core/blobstorage/vdisk/common
    )
END()
