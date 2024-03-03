#include "config.h"
#include "mount_config_storage.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/table_settings.h>

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerMasterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_tablet_balancer", &TThis::EnableTabletBalancer)
        .Default(true);
    registrar.Parameter("tablet_balancer_schedule", &TThis::TabletBalancerSchedule)
        .Default(DefaultTabletBalancerSchedule);
    registrar.Parameter("config_check_period", &TThis::ConfigCheckPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("balance_period", &TThis::BalancePeriod)
        .Default(TDuration::Minutes(5));

    registrar.Postprocessor([] (TThis* config) {
        if (config->TabletBalancerSchedule.IsEmpty()) {
            THROW_ERROR_EXCEPTION("tablet_balancer_schedule cannot be empty in master config");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTabletCellDecommissionerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_tablet_cell_decommission", &TThis::EnableTabletCellDecommission)
        .Default(true);
    registrar.Parameter("enable_tablet_cell_removal", &TThis::EnableTabletCellRemoval)
        .Default(true);
    registrar.Parameter("decommission_check_period", &TThis::DecommissionCheckPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("orphans_check_period", &TThis::OrphansCheckPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("decommission_throttler", &TThis::DecommissionThrottler)
        .DefaultNew();
    registrar.Parameter("kick_orphans_throttler", &TThis::KickOrphansThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletActionManagerMasterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_actions_cleanup_period", &TThis::TabletActionsCleanupPeriod)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTablesMulticellGossipConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_cell_statistics_gossip_period", &TThis::TabletCellStatisticsGossipPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("tablet_cell_status_full_gossip_period", &TThis::TabletCellStatusFullGossipPeriod)
        .Default();
    registrar.Parameter("tablet_cell_status_incremental_gossip_period", &TThis::TabletCellStatusIncrementalGossipPeriod)
        .Default();
    registrar.Parameter("table_statistics_gossip_period", &TThis::TableStatisticsGossipPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("table_statistics_gossip_throttler", &TThis::TableStatisticsGossipThrottler)
        .DefaultNew();
    registrar.Parameter("bundle_resource_usage_gossip_period", &TThis::BundleResourceUsageGossipPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("enable_update_statistics_on_heartbeat", &TThis::EnableUpdateStatisticsOnHeartbeat)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTabletCellBalancerMasterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_tablet_cell_smoothing", &TThis::EnableTabletCellSmoothing)
        .Default(true);
    registrar.Parameter("enable_verbose_logging", &TThis::EnableVerboseLogging)
        .Default(false);
    registrar.Parameter("rebalance_wait_time", &TThis::RebalanceWaitTime)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("checker_thread_count", &TThis::CheckerThreadCount)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTabletNodeTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_heartbeats", &TThis::MaxConcurrentHeartbeats)
        .Default(10)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCellHydraPersistenceSynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("use_hydra_persistence_directory", &TThis::UseHydraPersistenceDirectory)
        .Default(false);
    registrar.Parameter("migrate_to_virtual_cell_maps", &TThis::MigrateToVirtualCellMaps)
        .Default(false);
    registrar.Parameter("synchronization_period", &TThis::SynchronizationPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("max_cells_to_register_in_cypress_per_iteration", &TThis::MaxCellsToRegisterInCypressPerIteration)
        .GreaterThanOrEqual(0)
        .Default(200);
    registrar.Parameter("max_cells_to_unregister_from_cypress_per_iteration", &TThis::MaxCellsToUnregisterFromCypressPerIteration)
        .GreaterThanOrEqual(0)
        .Default(200);
    registrar.Parameter("max_cell_acl_updates_per_iteration", &TThis::MaxCellAclUpdatesPerIteration)
        .GreaterThanOrEqual(0)
        .Default(20);
    registrar.Parameter("hydra_persistence_file_id_update_period", &TThis::HydraPersistenceFileIdUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_hydra_persistence_file_id_updates_per_iteration", &TThis::MaxHydraPersistenceFileIdUpdatesPerIteration)
        .GreaterThan(0)
        .Default(200);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTabletManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("peer_revocation_timeout", &TThis::PeerRevocationTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("leader_reassignment_timeout", &TThis::LeaderReassignmentTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("max_snapshot_count_to_remove_per_check", &TThis::MaxSnapshotCountToRemovePerCheck)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("max_changelog_count_to_remove_per_check", &TThis::MaxChangelogCountToRemovePerCheck)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("safe_online_node_count", &TThis::SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(0);
    registrar.Parameter("cell_scan_period", &TThis::CellScanPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("enable_cell_tracker", &TThis::EnableCellTracker)
        .Default(true);
    registrar.Parameter("tablet_data_size_footprint", &TThis::TabletDataSizeFootprint)
        .GreaterThanOrEqual(0)
        .Default(64_MB);
    registrar.Parameter("store_chunk_reader", &TThis::StoreChunkReader)
        .Alias("chunk_reader")
        .DefaultNew();
    registrar.Parameter("hunk_chunk_reader", &TThis::HunkChunkReader)
        .DefaultNew();
    registrar.Parameter("store_chunk_writer", &TThis::StoreChunkWriter)
        .Alias("chunk_writer")
        .DefaultNew();
    registrar.Parameter("hunk_chunk_writer", &TThis::HunkChunkWriter)
        .DefaultNew();
    registrar.Parameter("tablet_balancer", &TThis::TabletBalancer)
        .DefaultNew();
    registrar.Parameter("hunk_store_writer", &TThis::HunkStoreWriter)
        .DefaultNew();
    registrar.Parameter("tablet_cell_decommissioner", &TThis::TabletCellDecommissioner)
        .DefaultNew();
    registrar.Parameter("tablet_action_manager", &TThis::TabletActionManager)
        .DefaultNew();
    registrar.Parameter("multicell_gossip", &TThis::MulticellGossip)
        // COMPAT(babenko)
        .Alias("multicell_gossip_config")
        .DefaultNew();
    registrar.Parameter("tablet_cells_cleanup_period", &TThis::TabletCellsCleanupPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("dynamic_table_profiling_mode", &TThis::DynamicTableProfilingMode)
        .Default(NTabletNode::EDynamicTableProfilingMode::Path);
    registrar.Parameter("tablet_cell_balancer", &TThis::TabletCellBalancer)
        .DefaultNew();
    registrar.Parameter("replicated_table_tracker", &TThis::ReplicatedTableTracker)
        .DefaultNew();
    registrar.Parameter("enable_bulk_insert", &TThis::EnableBulkInsert)
        .Default(false);
    registrar.Parameter("decommission_through_extra_peers", &TThis::DecommissionThroughExtraPeers)
        .Default(false);
    registrar.Parameter("synchronize_tablet_cell_leader_switches", &TThis::SynchronizeTabletCellLeaderSwitches)
        .Default(true)
        .DontSerializeDefault();
    registrar.Parameter("decommissioned_leader_reassignment_timeout", &TThis::DecommissionedLeaderReassignmentTimeout)
        .Default();
    registrar.Parameter("abandon_leader_lease_during_recovery", &TThis::AbandonLeaderLeaseDuringRecovery)
        .Default(false);
    registrar.Parameter("enable_dynamic_store_read_by_default", &TThis::EnableDynamicStoreReadByDefault)
        .Default(false);
    registrar.Parameter("peer_revocation_reason_expiration_time", &TThis::PeerRevocationReasonExpirationTime)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("enable_relaxed_tablet_statistics_validation", &TThis::EnableRelaxedTabletStatisticsValidation)
        .Default(false);
    registrar.Parameter("enable_aggressive_tablet_statistics_validation", &TThis::EnableAggressiveTabletStatisticsValidation)
        .Default(false);
    registrar.Parameter("extra_peer_drop_delay", &TThis::ExtraPeerDropDelay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("accumulate_preload_pending_store_count_correctly", &TThis::AccumulatePreloadPendingStoreCountCorrectly)
        .Default(false)
        .DontSerializeDefault();
    registrar.Parameter("increase_upload_replication_factor", &TThis::IncreaseUploadReplicationFactor)
        .Default(false);
    registrar.Parameter("enable_tablet_resource_validation", &TThis::EnableTabletResourceValidation)
        .Default(false);

    registrar.Parameter("tablet_node_tracker", &TThis::TabletNodeTracker)
        .DefaultNew();

    registrar.Parameter("enable_backups", &TThis::EnableBackups)
        .Default(false);

    registrar.Parameter("send_dynamic_store_id_in_backup", &TThis::SendDynamicStoreIdInBackup)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("include_mount_config_attributes_in_user_attributes", &TThis::IncludeMountConfigAttributesInUserAttributes)
        .Default(true);

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("tamed_cell_manager_profiling_period", &TThis::TamedCellManagerProfilingPeriod)
        .Default(DefaultTamedCellManagerProfilingPeriod);

    registrar.Parameter("cell_hydra_persistence_synchronizer", &TThis::CellHydraPersistenceSynchronizer)
        .DefaultNew();

    registrar.Parameter("forbid_arbitrary_data_versions_in_retention_config", &TThis::ForbidArbitraryDataVersionsInRetentionConfig)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("max_table_collocation_size", &TThis::MaxTableCollocationSize)
        .Default(200);

    registrar.Parameter("add_perf_counters_to_tablets_attribute", &TThis::AddPerfCountersToTabletsAttribute)
        .Default(true);

    registrar.Parameter("use_avenues", &TThis::UseAvenues)
        .Default(false);

    registrar.Parameter("replicate_table_collocations", &TThis::ReplicateTableCollocations)
        .Default(true)
        .DontSerializeDefault();

    registrar.Parameter("max_chunks_per_mounted_tablet", &TThis::MaxChunksPerMountedTablet)
        .Default(15000);

    registrar.Preprocessor([] (TThis* config) {
        config->StoreChunkReader->SuspiciousNodeGracePeriod = TDuration::Minutes(5);
        config->StoreChunkReader->BanPeersPermanently = false;

        config->StoreChunkWriter->BlockSize = 256_KB;
        config->StoreChunkWriter->SampleRate = 0.0005;
    });

    registrar.Postprocessor([] (TThis* config) {
        config->MaxSnapshotCountToKeep = 2;

        for (const auto& [name, experiment] : config->TableConfigExperiments) {
            if (experiment->Salt.empty()) {
                experiment->Salt = name;
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
