LIBRARY()

SRCS(
    fast_path_service.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/ddisk
    contrib/ydb/core/mind/bscontroller

    contrib/ydb/core/nbs/cloud/blockstore/config
    contrib/ydb/core/nbs/cloud/blockstore/libs/service
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group
)

END()
