LIBRARY()

SRCS(
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_createvolume.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_waitschemetx.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/api
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/core
    contrib/ydb/core/nbs/cloud/storage/core/libs/kikimr

    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
)
