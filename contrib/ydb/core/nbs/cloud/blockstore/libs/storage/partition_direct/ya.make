LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    contrib/ydb/library/actors/core
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/api
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
)

END()
