LIBRARY()

SRCS(
    coordinator.cpp
    coordinator_hooks.cpp
    coordinator_impl.cpp
    coordinator_state.cpp
    coordinator__acquire_read_step.cpp
    coordinator__configure.cpp
    coordinator__check.cpp
    coordinator__init.cpp
    coordinator__last_step_subscriptions.cpp
    coordinator__mediators_confirmations.cpp
    coordinator__monitoring.cpp
    coordinator__plan_step.cpp
    coordinator__read_step_subscriptions.cpp
    coordinator__restore_params.cpp
    coordinator__restore_transaction.cpp
    coordinator__schema.cpp
    coordinator__schema_upgrade.cpp
    coordinator__stop_guard.cpp
    defs.h
    mediator_queue.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    contrib/ydb/library/actors/interconnect
    library/cpp/containers/absl_flat_hash
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx
    contrib/ydb/core/tx/coordinator/public
    contrib/ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
