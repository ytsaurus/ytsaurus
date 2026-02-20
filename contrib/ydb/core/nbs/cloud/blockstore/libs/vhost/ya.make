LIBRARY()

SRCS(
    server.cpp
    vhost.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/libs/diagnostics
    contrib/ydb/core/nbs/cloud/blockstore/libs/service

    contrib/ydb/core/nbs/cloud/storage/core/libs/common

    contrib/ydb/core/nbs/cloud/contrib/vhost

    contrib/ydb/library/actors/core

    library/cpp/logger
    library/cpp/unified_agent_client
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_stress
)
