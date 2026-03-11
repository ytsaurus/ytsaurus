LIBRARY()

SRCS(
    direct_block_group_in_mem.cpp
    direct_block_group.cpp
    fast_path_service.cpp
    load_actor_adapter.cpp
    partition_direct_actor.cpp
    partition_direct.cpp
    request.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/bootstrap
    contrib/ydb/core/nbs/cloud/blockstore/config
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/api
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    contrib/ydb/core/nbs/cloud/storage/core/libs/coroutine

    contrib/ydb/core/protos
    contrib/ydb/library/services

    contrib/ydb/core/mind/bscontroller
)

END()

RECURSE_FOR_TESTS(
    ut
)
