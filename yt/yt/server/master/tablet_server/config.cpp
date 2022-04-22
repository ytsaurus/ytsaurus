#include "config.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

TTabletBalancerConfig::TTabletBalancerConfig()
{
    RegisterParameter("enable_in_memory_cell_balancer", EnableInMemoryCellBalancer)
        .Default(true)
        .Alias("enable_in_memory_balancer");

    RegisterParameter("enable_cell_balancer", EnableCellBalancer)
        .Default(false);

    RegisterParameter("enable_tablet_size_balancer", EnableTabletSizeBalancer)
        .Default(true);

    // COMPAT(savrus) Only for compatibility purpose.
    RegisterParameter("compat_enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
        .Default(true)
        .Alias("enable_tablet_cell_smoothing");

    RegisterParameter("soft_in_memory_cell_balance_threshold", SoftInMemoryCellBalanceThreshold)
        .Default(0.05)
        .Alias("cell_balance_factor");

    RegisterParameter("hard_in_memory_cell_balance_threshold", HardInMemoryCellBalanceThreshold)
        .Default(0.15);

    RegisterParameter("min_tablet_size", MinTabletSize)
        .Default(128_MB);

    RegisterParameter("max_tablet_size", MaxTabletSize)
        .Default(20_GB);

    RegisterParameter("desired_tablet_size", DesiredTabletSize)
        .Default(10_GB);

    RegisterParameter("min_in_memory_tablet_size", MinInMemoryTabletSize)
        .Default(512_MB);

    RegisterParameter("max_in_memory_tablet_size", MaxInMemoryTabletSize)
        .Default(2_GB);

    RegisterParameter("desired_in_memory_tablet_size", DesiredInMemoryTabletSize)
        .Default(1_GB);

    RegisterParameter("tablet_to_cell_ratio", TabletToCellRatio)
        .GreaterThan(0)
        .Default(5.0);

    RegisterParameter("tablet_balancer_schedule", TabletBalancerSchedule)
        .Default();

    RegisterParameter("enable_verbose_logging", EnableVerboseLogging)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (MinTabletSize > DesiredTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
        }
        if (DesiredTabletSize > MaxTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
        }
        if (MinInMemoryTabletSize >= DesiredInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_in_memory_tablet_size\" must be less than \"desired_in_memory_tablet_size\"");
        }
        if (DesiredInMemoryTabletSize >= MaxInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_in_memory_tablet_size\" must be less than \"max_in_memory_tablet_size\"");
        }
        if (SoftInMemoryCellBalanceThreshold > HardInMemoryCellBalanceThreshold) {
            THROW_ERROR_EXCEPTION("\"soft_in_memory_cell_balance_threshold\" must less than or equal to "
                "\"hard_in_memory_cell_balance_threshold\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTabletBalancerMasterConfig::TTabletBalancerMasterConfig()
{
    RegisterParameter("enable_tablet_balancer", EnableTabletBalancer)
        .Default(true);
    RegisterParameter("tablet_balancer_schedule", TabletBalancerSchedule)
        .Default(DefaultTabletBalancerSchedule);
    RegisterParameter("config_check_period", ConfigCheckPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("balance_period", BalancePeriod)
        .Default(TDuration::Minutes(5));

    RegisterPostprocessor([&] () {
        if (TabletBalancerSchedule.IsEmpty()) {
            THROW_ERROR_EXCEPTION("tablet_balancer_schedule cannot be empty in master config");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellDecommissionerConfig::TTabletCellDecommissionerConfig()
{
    RegisterParameter("enable_tablet_cell_decommission", EnableTabletCellDecommission)
        .Default(true);
    RegisterParameter("enable_tablet_cell_removal", EnableTabletCellRemoval)
        .Default(true);
    RegisterParameter("decommission_check_period", DecommissionCheckPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("orphans_check_period", OrphansCheckPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("decommission_throttler", DecommissionThrottler)
        .DefaultNew();
    RegisterParameter("kick_orphans_throttler", KickOrphansThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TTabletActionManagerMasterConfig::TTabletActionManagerMasterConfig()
{
    RegisterParameter("tablet_actions_cleanup_period", TabletActionsCleanupPeriod)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTablesMulticellGossipConfig::TDynamicTablesMulticellGossipConfig()
{
    RegisterParameter("tablet_cell_statistics_gossip_period", TabletCellStatisticsGossipPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("tablet_cell_status_full_gossip_period", TabletCellStatusFullGossipPeriod)
        .Default();
    RegisterParameter("tablet_cell_status_incremental_gossip_period", TabletCellStatusIncrementalGossipPeriod)
        .Default();
    RegisterParameter("table_statistics_gossip_period", TableStatisticsGossipPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("table_statistics_gossip_throttler", TableStatisticsGossipThrottler)
        .DefaultNew();
    RegisterParameter("bundle_resource_usage_gossip_period", BundleResourceUsageGossipPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("enable_update_statistics_on_heartbeat", EnableUpdateStatisticsOnHeartbeat)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTabletCellBalancerMasterConfig::TDynamicTabletCellBalancerMasterConfig()
{
    RegisterParameter("enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
        .Default(true);
    RegisterParameter("enable_verbose_logging", EnableVerboseLogging)
        .Default(false);
    RegisterParameter("rebalance_wait_time", RebalanceWaitTime)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableTrackerExpiringCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->RefreshTime = std::nullopt;
        config->ExpireAfterAccessTime = TDuration::Seconds(1);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(1);
        config->ExpireAfterFailedUpdateTime = TDuration::Seconds(1);
    });
}

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableTrackerConfig::TReplicatedTableTrackerConfig()
{
    RegisterParameter("checker_thread_count", CheckerThreadCount)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicReplicatedTableTrackerConfig::TDynamicReplicatedTableTrackerConfig()
{
    RegisterParameter("enable_replicated_table_tracker", EnableReplicatedTableTracker)
        .Default(true);
    RegisterParameter("check_period", CheckPeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("general_check_timeout", GeneralCheckTimeout)
        .Default(TDuration::Minutes(1))
        .DontSerializeDefault();
    RegisterParameter("replicator_hint", ReplicatorHint)
        .DefaultNew();
    RegisterParameter("bundle_health_cache", BundleHealthCache)
        .DefaultNew();
    RegisterParameter("cluster_state_cache", ClusterStateCache)
        .DefaultNew();
    RegisterParameter("cluster_directory_synchronizer", ClusterDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("max_iterations_without_acceptable_bundle_health", MaxIterationsWithoutAcceptableBundleHealth)
        .Default(1)
        .DontSerializeDefault();

    RegisterPreprocessor([&] {
        ClusterStateCache->RefreshTime = CheckPeriod;
    });
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTabletNodeTrackerConfig::TDynamicTabletNodeTrackerConfig()
{
    RegisterParameter("max_concurrent_heartbeats", MaxConcurrentHeartbeats)
        .Default(10)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTabletManagerConfig::TDynamicTabletManagerConfig()
{
    RegisterParameter("peer_revocation_timeout", PeerRevocationTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("leader_reassignment_timeout", LeaderReassignmentTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("max_snapshot_count_to_remove_per_check", MaxSnapshotCountToRemovePerCheck)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("max_changelog_count_to_remove_per_check", MaxChangelogCountToRemovePerCheck)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(0);
    RegisterParameter("cell_scan_period", CellScanPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("enable_cell_tracker", EnableCellTracker)
        .Default(true);
    RegisterParameter("tablet_data_size_footprint", TabletDataSizeFootprint)
        .GreaterThanOrEqual(0)
        .Default(64_MB);
    RegisterParameter("store_chunk_reader", StoreChunkReader)
        .Alias("chunk_reader")
        .DefaultNew();
    RegisterParameter("hunk_chunk_reader", HunkChunkReader)
        .DefaultNew();
    RegisterParameter("store_chunk_writer", StoreChunkWriter)
        .Alias("chunk_writer")
        .DefaultNew();
    RegisterParameter("hunk_chunk_writer", HunkChunkWriter)
        .DefaultNew();
    RegisterParameter("tablet_balancer", TabletBalancer)
        .DefaultNew();
    RegisterParameter("tablet_cell_decommissioner", TabletCellDecommissioner)
        .DefaultNew();
    RegisterParameter("tablet_action_manager", TabletActionManager)
        .DefaultNew();
    RegisterParameter("multicell_gossip", MulticellGossip)
        // COMPAT(babenko)
        .Alias("multicell_gossip_config")
        .DefaultNew();
    RegisterParameter("tablet_cells_cleanup_period", TabletCellsCleanupPeriod)
        .Default(TDuration::Seconds(60));
    RegisterParameter("dynamic_table_profiling_mode", DynamicTableProfilingMode)
        .Default(NTabletNode::EDynamicTableProfilingMode::Path);
    RegisterParameter("tablet_cell_balancer", TabletCellBalancer)
        .DefaultNew();
    RegisterParameter("replicated_table_tracker", ReplicatedTableTracker)
        .DefaultNew();
    RegisterParameter("enable_bulk_insert", EnableBulkInsert)
        .Default(false);
    RegisterParameter("decommission_through_extra_peers", DecommissionThroughExtraPeers)
        .Default(false);
    RegisterParameter("decommissioned_leader_reassignment_timeout", DecommissionedLeaderReassignmentTimeout)
        .Default();
    RegisterParameter("abandon_leader_lease_during_recovery", AbandonLeaderLeaseDuringRecovery)
        .Default(false);
    RegisterParameter("enable_dynamic_store_read_by_default", EnableDynamicStoreReadByDefault)
        .Default(false);
    RegisterParameter("peer_revocation_reason_expiration_time", PeerRevocationReasonExpirationTime)
        .Default(TDuration::Minutes(15));
    RegisterParameter("enable_relaxed_tablet_statistics_validation", EnableRelaxedTabletStatisticsValidation)
        .Default(false);
    RegisterParameter("enable_aggressive_tablet_statistics_validation", EnableAggressiveTabletStatisticsValidation)
        .Default(false);
    RegisterParameter("extra_peer_drop_delay", ExtraPeerDropDelay)
        .Default(TDuration::Minutes(1));
    RegisterParameter("accumulate_preload_pending_store_count_correctly", AccumulatePreloadPendingStoreCountCorrectly)
        .Default(false)
        .DontSerializeDefault();
    RegisterParameter("increase_upload_replication_factor", IncreaseUploadReplicationFactor)
        .Default(false);
    RegisterParameter("enable_tablet_resource_validation", EnableTabletResourceValidation)
        .Default(false);

    RegisterParameter("tablet_node_tracker", TabletNodeTracker)
        .DefaultNew();

    RegisterParameter("enable_hunks", EnableHunks)
        .Default(false);

    RegisterParameter("profiling_period", ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    RegisterParameter("tamed_cell_manager_profiling_period", TamedCellManagerProfilingPeriod)
        .Default(DefaultTamedCellManagerProfilingPeriod);

   RegisterPreprocessor([&] {
        StoreChunkReader->SuspiciousNodeGracePeriod = TDuration::Minutes(5);
        StoreChunkReader->BanPeersPermanently = false;

        StoreChunkWriter->BlockSize = 256_KB;
        StoreChunkWriter->SampleRate = 0.0005;
    });

    RegisterPostprocessor([&] {
        MaxSnapshotCountToKeep = 2;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
