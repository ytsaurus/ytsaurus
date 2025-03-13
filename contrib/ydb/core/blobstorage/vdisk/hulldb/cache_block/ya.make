LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/protos
    contrib/ydb/core/protos
)

SRCS(
    cache_block.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
