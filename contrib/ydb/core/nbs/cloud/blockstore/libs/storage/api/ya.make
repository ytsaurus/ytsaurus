LIBRARY()

SRCS(
    service.cpp
    ss_proxy.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/public/api/protos
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
)

END()
