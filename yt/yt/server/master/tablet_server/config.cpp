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

TReplicatedTableTrackerConfig::TReplicatedTableTrackerConfig()
{
    RegisterParameter("checker_thread_count", CheckerThreadCount)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTabletNodeTrackerConfig::TDynamicTabletNodeTrackerConfig()
{
    RegisterParameter("max_concurrent_heartbeats", MaxConcurrentHeartbeats)
        .Default(10)
        .GreaterThan(0);
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

    registrar.Parameter("enable_hunks", &TThis::EnableHunks)
        .Default(false);

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("tamed_cell_manager_profiling_period", &TThis::TamedCellManagerProfilingPeriod)
        .Default(DefaultTamedCellManagerProfilingPeriod);

   registrar.Preprocessor([] (TThis* config) {
        config->StoreChunkReader->SuspiciousNodeGracePeriod = TDuration::Minutes(5);
        config->StoreChunkReader->BanPeersPermanently = false;

        config->StoreChunkWriter->BlockSize = 256_KB;
        config->StoreChunkWriter->SampleRate = 0.0005;
    });

    registrar.Postprocessor([] (TThis* config) {
        config->MaxSnapshotCountToKeep = 2;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
