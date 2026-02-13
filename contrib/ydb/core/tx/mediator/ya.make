LIBRARY()

SRCS(
    mediator.cpp
    mediator_impl.cpp
    mediator__init.cpp
    mediator__configure.cpp
    mediator__schema.cpp
    mediator__schema_upgrade.cpp
    tablet_queue.cpp
    execute_queue.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/scheme_types
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx
    contrib/ydb/core/tx/coordinator/public
    contrib/ydb/core/tx/time_cast
    contrib/ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
