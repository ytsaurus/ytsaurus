LIBRARY()

    SRCS(
        bridge_proxy.cpp
        bridge_proxy.h
        defs.h
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/dsproxy
        contrib/ydb/core/blobstorage/groupinfo
    )

END()
