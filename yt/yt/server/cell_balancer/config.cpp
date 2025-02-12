#include "config.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_manager", &TThis::TabletManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("connect_retry_backoff_time", &TThis::ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TChaosConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_cell_clusters", &TThis::TabletCellClusters)
        .Default();
    registrar.Parameter("chaos_cell_clusters", &TThis::ChaosCellClusters)
        .Default();
    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Default();
    registrar.Parameter("alpha_chaos_cluster", &TThis::AlphaChaosCluster)
        .Default();
    registrar.Parameter("beta_chaos_cluster", &TThis::BetaChaosCluster)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBundleControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster)
        .NonEmpty();

    registrar.Parameter("bundle_scan_period", &TThis::BundleScanPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("bundle_scan_transaction_timeout", &TThis::BundleScanTransactionTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("hulk_request_timeout", &TThis::HulkRequestTimeout)
        .Default(TDuration::Hours(1));
    registrar.Parameter("cell_removal_timeout", &TThis::CellRemovalTimeout)
        .Default(TDuration::Hours(1));
    registrar.Parameter("spare_node_assignment_timeout", &TThis::NodeAssignmentTimeout)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("mute_tablet_cells_check_grace_period", &TThis::MuteTabletCellsCheckGracePeriod)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("root_path", &TThis::RootPath)
        .NonEmpty();

    registrar.Parameter("has_instance_allocator_service", &TThis::HasInstanceAllocatorService)
        .Default(true);
    registrar.Parameter("hulk_allocations_path", &TThis::HulkAllocationsPath)
        .NonEmpty();
    registrar.Parameter("hulk_allocations_history_path", &TThis::HulkAllocationsHistoryPath)
        .NonEmpty();
    registrar.Parameter("hulk_deallocations_path", &TThis::HulkDeallocationsPath)
        .NonEmpty();
    registrar.Parameter("hulk_deallocations_history_path", &TThis::HulkDeallocationsHistoryPath)
        .NonEmpty();

    registrar.Parameter("decommission_released_nodes", &TThis::DecommissionReleasedNodes)
        .Default(true);

    registrar.Parameter("node_count_per_cell", &TThis::NodeCountPerCell)
        .GreaterThan(0)
        .Default(25);
    registrar.Parameter("chunk_count_per_cell", &TThis::ChunkCountPerCell)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("journal_disk_space_per_cell", &TThis::JournalDiskSpacePerCell)
        .GreaterThan(0)
        .Default(100_GB);
    registrar.Parameter("snapshot_disk_space_per_cell", &TThis::SnapshotDiskSpacePerCell)
        .GreaterThan(0)
        .Default(15_GB);
    registrar.Parameter("min_node_count", &TThis::MinNodeCount)
        .GreaterThan(0)
        .Default(1000);
    registrar.Parameter("min_chunk_count", &TThis::MinChunkCount)
        .GreaterThan(0)
        .Default(1000);

    registrar.Parameter("reallocate_instance_budget", &TThis::ReallocateInstanceBudget)
        .GreaterThan(0)
        .Default(20);

    registrar.Parameter("remove_instance_cypress_node_after", &TThis::RemoveInstanceCypressNodeAfter)
        .Default(TDuration::Days(7));

    registrar.Parameter("offline_instance_grace_period", &TThis::OfflineInstanceGracePeriod)
        .Default(TDuration::Minutes(40));

    registrar.Parameter("enable_network_limits", &TThis::EnableNetworkLimits)
        .Default(false);

    registrar.Parameter("skip_jailed_bundles", &TThis::SkipJailedBundles)
        .Default(true);

    registrar.Parameter("enable_chaos_bundle_management", &TThis::EnableChaosBundleManagement)
        .Default(false);

    registrar.Parameter("chaos_config", &TThis::ChaosConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();

    registrar.Parameter("enable_cell_balancer", &TThis::EnableCellBalancer)
        .Default(true);
    registrar.Parameter("cell_balancer", &TThis::CellBalancer)
        .DefaultNew();

    registrar.Parameter("enable_bundle_controller", &TThis::EnableBundleController)
        .Default(false);
    registrar.Parameter("bundle_controller", &TThis::BundleController)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
