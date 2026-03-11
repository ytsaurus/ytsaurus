LIBRARY()

SRCS(
    ic_storage_transport.cpp
)

PEERDIR(
    contrib/ydb/core/mind/bscontroller

    contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr
)

END()
