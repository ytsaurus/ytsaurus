LIBRARY()

SRCS(
    ic_storage_transport_actor.cpp
    ic_storage_transport_events.cpp
    ic_storage_transport.cpp
    storage_transport.cpp
)

PEERDIR(
    contrib/ydb/core/mind/bscontroller

    contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr
)

END()
