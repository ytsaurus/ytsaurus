LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    automaton.cpp
    bootstrap.cpp
    chaos_cell_synchronizer.cpp
    chaos_manager.cpp
    chaos_manager.proto
    chaos_node_service.cpp
    chaos_slot.cpp
    coordinator_manager.cpp
    coordinator_service.cpp
    foreign_migrated_replication_card_remover.cpp
    migrated_replication_card_remover.cpp
    replication_card.cpp
    replication_card_collocation.cpp
    replication_card_observer.cpp
    serialize.cpp
    shortcut_snapshot_store.cpp
    slot_manager.cpp
    slot_provider.cpp
    transaction.cpp
    transaction_manager.cpp
    replicated_table_tracker.cpp
    replication_card_serialization.cpp
)

PEERDIR(
    yt/yt/server/lib/hive
    yt/yt/server/lib/chaos_node
    yt/yt/server/lib/transaction_supervisor
)

END()
