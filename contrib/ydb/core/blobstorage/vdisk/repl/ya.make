LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb/barriers
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_hullrepljob.cpp
    blobstorage_hullrepljob.h
    blobstorage_hullreplwritesst.h
    blobstorage_replbroker.cpp
    blobstorage_replbroker.h
    blobstorage_repl.cpp
    blobstorage_replctx.h
    blobstorage_repl.h
    blobstorage_replproxy.cpp
    blobstorage_replproxy.h
    blobstorage_replrecoverymachine.h
    blobstorage_replmonhandler.cpp
    defs.h
    query_donor.h
    repl_quoter.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
