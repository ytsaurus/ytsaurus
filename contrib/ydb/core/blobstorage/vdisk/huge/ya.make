LIBRARY()

PEERDIR(
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
)

SRCS(
    blobstorage_hullhuge.cpp
    blobstorage_hullhugedefs.cpp
    blobstorage_hullhugedefs.h
    blobstorage_hullhuge.h
    blobstorage_hullhugeheap.cpp
    blobstorage_hullhugeheap.h
    blobstorage_hullhugerecovery.cpp
    blobstorage_hullhugerecovery.h
    booltt.h
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
