#include "config.h"

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

void TChaosCellSynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardObserverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("observation_period", &TThis::ObservationPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("replication_card_count_per_round", &TThis::ReplicationCardCountPerRound)
        .GreaterThan(0)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

void TMigratedReplicationCardRemoverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("remove_period", &TThis::RemovePeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TForeignMigratedReplicationCardRemoverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("remove_period", &TThis::RemovePeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("replication_card_keep_alive_period", &TThis::ReplicationCardKeepAlivePeriod)
        .Default(TDuration::Days(30));
}

////////////////////////////////////////////////////////////////////////////////

void TChaosManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chaos_cell_synchronizer", &TThis::ChaosCellSynchronizer)
        .DefaultNew();
    registrar.Parameter("replication_card_observer", &TThis::ReplicationCardObserver)
        .DefaultNew();
    registrar.Parameter("era_commencing_period", &TThis::EraCommencingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("migrated_replication_card_remover", &TThis::MigratedReplicationCardRemover)
        .DefaultNew();
    registrar.Parameter("foreign_migrated_replication_card_remover", &TThis::ForeignMigratedReplicationCardRemover)
        .DefaultNew();
}
////////////////////////////////////////////////////////////////////////////////

void TTransactionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_transaction_timeout", &TThis::MaxTransactionTimeout)
        .GreaterThan(TDuration())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("max_aborted_transaction_pool_size", &TThis::MaxAbortedTransactionPoolSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

void TChaosNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_occupant", &TThis::CellarOccupant)
        .DefaultNew();
    registrar.Parameter("transaction_manager", &TThis::TransactionManager)
        .DefaultNew();
    registrar.Parameter("chaos_manager", &TThis::ChaosManager)
        .DefaultNew();
    registrar.Parameter("coordinator_manager", &TThis::CoordinatorManager)
        .DefaultNew();
    registrar.Parameter("slot_scan_period", &TThis::SlotScanPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("snapshot_store_read_pool_size", &TThis::SnapshotStoreReadPoolSize)
        .Default(8);
    registrar.Parameter("replicated_table_tracker_config_fetcher", &TThis::ReplicatedTableTrackerConfigFetcher)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
