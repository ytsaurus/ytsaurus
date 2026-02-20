LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/services

    contrib/ydb/core/nbs/cloud/blockstore/config
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/load_actor_adapter
)

END()

RECURSE_FOR_TESTS(
    ut
)
