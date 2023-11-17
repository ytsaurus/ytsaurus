LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/vdisk/hulldb/barriers
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_anubis.cpp
    blobstorage_anubis_algo.cpp
    blobstorage_anubis_osiris.cpp
    blobstorage_anubisfinder.cpp
    blobstorage_anubisproxy.cpp
    blobstorage_anubisrunner.cpp
    blobstorage_osiris.cpp
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
