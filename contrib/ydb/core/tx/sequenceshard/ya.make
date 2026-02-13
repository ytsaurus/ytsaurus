LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/erasure
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/sequenceshard/public
)

SRCS(
    schema.cpp
    sequenceshard.cpp
    sequenceshard_impl.cpp
    tx_allocate_sequence.cpp
    tx_create_sequence.cpp
    tx_drop_sequence.cpp
    tx_freeze_sequence.cpp
    tx_get_sequence.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_mark_schemeshard_pipe.cpp
    tx_redirect_sequence.cpp
    tx_restore_sequence.cpp
    tx_update_sequence.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    public
)

RECURSE_FOR_TESTS(
    ut
)
