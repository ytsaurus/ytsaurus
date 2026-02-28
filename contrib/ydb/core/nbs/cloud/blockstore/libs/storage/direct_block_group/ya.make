LIBRARY()

SRCS(
    direct_block_group.cpp
    request.cpp
)

PEERDIR(
    contrib/ydb/core/mind/bscontroller

    contrib/ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport
)

END()
