LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/public/api/protos
    contrib/ydb/library/actors/core
)

END()
