LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/tx_allocator
)

SRCS(
    actor_client.cpp
    client.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
