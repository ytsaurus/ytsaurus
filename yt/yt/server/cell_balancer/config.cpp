#include "config.h"

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_manager", &TThis::TabletManager)
        .DefaultNew();
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

    registrar.Parameter("use_dedicated_user_name", &TThis::UseDedicatedUserName)
        .Default();

    registrar.Parameter("bundle_scan_period", &TThis::BundleScanPeriod)
        .Default(TDuration::Seconds(10));

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
    registrar.Parameter("decommissioned_node_drain_timeout", &TThis::DecommissionedNodeDrainTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("root_path", &TThis::RootPath)
        .Default("//sys/bundle_controller/controller")
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
    registrar.Parameter("enable_spare_node_assignment", &TThis::EnableSpareNodeAssignment)
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

    registrar.Parameter("annotate_new_nodes", &TThis::AnnotateNewNodes)
        .Default(false);
    registrar.Parameter("annotate_new_proxies", &TThis::AnnotateNewProxies)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TNodeTrackerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TBundleControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_scan_period", &TThis::BundleScanPeriod)
        .Default();

    registrar.Parameter("node_tracker", &TThis::NodeTracker)
        .DefaultNew();

    registrar.Parameter("remove_tags_from_offline_nodes", &TThis::RemoveTagsFromOfflineNodes)
        .Default(false);

    registrar.Parameter("remove_instance_cypress_node_after", &TThis::RemoveInstanceCypressNodeAfter)
        .Default();
    registrar.Parameter("offline_instance_grace_period", &TThis::OfflineInstanceGracePeriod)
        .Default();

    registrar.Parameter("max_concurrent_cypress_write_requests", &TThis::MaxConcurrentCypressWriteRequests)
        .Default(50);
    registrar.Parameter("max_released_nodes_per_iteration", &TThis::MaxReleasedNodesPerIteration)
        .Default(50);

    registrar.Parameter("flush_log_after_mutations", &TThis::FlushLogAfterMutations)
        .Default(false);

    registrar.Parameter("enable_chaos_bundle_management", &TThis::EnableChaosBundleManagement)
        .Default();

    registrar.Parameter("foreign_cluster_request_timeout", &TThis::ForeignClusterRequestTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("annotate_new_nodes", &TThis::AnnotateNewNodes)
        .Default();
    registrar.Parameter("annotate_new_proxies", &TThis::AnnotateNewProxies)
        .Default();

    registrar.Parameter("use_data_node_racks", &TThis::UseDataNodeRacks)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultCtor([] {
            auto electionManager = New<NCypressElection::TCypressElectionManagerConfig>();
            electionManager->LockPath = "//sys/bundle_controller/coordinator/lock";
            return electionManager;
        });
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

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default(DefaultBundleControllerConfigPath);

    registrar.Preprocessor([] (TThis* config) {
        config->DynamicConfigManager->IgnoreConfigAbsence = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
