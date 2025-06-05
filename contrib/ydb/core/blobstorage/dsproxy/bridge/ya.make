LIBRARY()

    SRCS(
        bridge.cpp
        bridge.h
        defs.h
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/dsproxy
        contrib/ydb/core/blobstorage/groupinfo
    )

END()
