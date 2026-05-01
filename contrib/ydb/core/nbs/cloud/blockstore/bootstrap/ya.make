LIBRARY()

SRCS(
    bootstrap.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/vhost
    contrib/ydb/core/nbs/cloud/blockstore/config
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
    contrib/ydb/core/protos
)

END()
