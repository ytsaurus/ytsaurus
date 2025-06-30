LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/digest/crc32c
    contrib/ydb/core/base
    contrib/ydb/core/base/services
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/vdisk/ingress
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_groupinfo_blobmap.cpp
    blobstorage_groupinfo_blobmap.h
    blobstorage_groupinfo.cpp
    blobstorage_groupinfo.h
    blobstorage_groupinfo_data_check.h
    blobstorage_groupinfo_iter.h
    blobstorage_groupinfo_partlayout.cpp
    blobstorage_groupinfo_partlayout.h
    blobstorage_groupinfo_sets.h
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
