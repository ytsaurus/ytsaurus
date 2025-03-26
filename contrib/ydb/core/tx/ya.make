LIBRARY()

SRCS(
    defs.h
    message_seqno.cpp
    tx.h
    tx.cpp
    tx_processing.h
    tx_proxy_schemereq.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/persqueue/config
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/util
    contrib/ydb/library/aclib
)

END()

RECURSE(
    balance_coverage
    columnshard
    coordinator
    datashard
    locks
    long_tx_service
    mediator
    replication
    scheme_board
    scheme_cache
    schemeshard
    sequenceproxy
    sequenceshard
    sharding
    tiering
    time_cast
    tracing
    tx_allocator
    tx_allocator_client
    tx_proxy
)
