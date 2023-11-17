LIBRARY()

SRCS(
    storagepool_counters.h
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
