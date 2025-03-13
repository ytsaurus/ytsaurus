LIBRARY()

SRCS(
    table_creator.cpp
    table_creator.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
