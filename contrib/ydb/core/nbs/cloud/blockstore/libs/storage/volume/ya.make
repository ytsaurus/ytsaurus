LIBRARY()

SRCS(
    volume.cpp
    volume_actor.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr
    contrib/ydb/core/nbs/cloud/blockstore/libs/service
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/api
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/core
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct
    contrib/ydb/core/nbs/cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
    library/cpp/lwtrace
    library/cpp/monlib/service/pages
    library/cpp/protobuf/util

    contrib/ydb/core/base
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/mind
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
)

END()
