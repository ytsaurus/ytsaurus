#include "config.h"

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

void TNodeDiscoveryManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("peer_count", &TThis::PeerCount)
        .GreaterThanOrEqual(0)
        .Default(10);
    registrar.Parameter("max_peers_per_rack", &TThis::MaxPeersPerRack)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_node_transaction_timeout", &TThis::DefaultNodeTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("default_data_node_lease_transaction_timeout", &TThis::DefaultDataNodeLeaseTransactionTimeout)
        .Default(TDuration::Seconds(120));
}

////////////////////////////////////////////////////////////////////////////////

void TNodeGroupConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_node_registrations", &TThis::MaxConcurrentNodeRegistrations)
        .Default(20)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TNodeGroupConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicNodeTrackerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_disposal_finishing", &TThis::DisableDisposalFinishing)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicNodeTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("node_groups", &TThis::NodeGroups)
        .Default();

    registrar.Parameter("total_node_statistics_update_period", &TThis::TotalNodeStatisticsUpdatePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("full_node_states_gossip_period", &TThis::FullNodeStatesGossipPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("node_statistics_gossip_period", &TThis::NodeStatisticsGossipPeriod)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("max_concurrent_node_registrations", &TThis::MaxConcurrentNodeRegistrations)
        .Default(20)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_concurrent_node_unregistrations", &TThis::MaxConcurrentNodeUnregistrations)
        .Default(20)
        .GreaterThan(0);

    registrar.Parameter("max_concurrent_cluster_node_heartbeats", &TThis::MaxConcurrentClusterNodeHeartbeats)
        .Default(50)
        .GreaterThan(0);
    registrar.Parameter("max_concurrent_exec_node_heartbeats", &TThis::MaxConcurrentExecNodeHeartbeats)
        .Default(50)
        .GreaterThan(0);

    registrar.Parameter("force_node_heartbeat_request_timeout", &TThis::ForceNodeHeartbeatRequestTimeout)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("master_cache_manager", &TThis::MasterCacheManager)
        .DefaultNew();
    registrar.Parameter("timestamp_provider_manager", &TThis::TimestampProviderManager)
        .DefaultNew();

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();

    registrar.Parameter("enable_structured_log", &TThis::EnableStructuredLog)
        .Default(false);

    registrar.Parameter("enable_node_cpu_statistics", &TThis::EnableNodeCpuStatistics)
        .Default(true);

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);

    registrar.Parameter(
        "use_resource_statistics_from_cluster_node_heartbeat",
        &TThis::UseResourceStatisticsFromClusterNodeHeartbeat)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_real_chunk_locations", &TThis::EnableRealChunkLocations)
        .Default(true);

    registrar.Parameter("forbid_maintenance_attribute_writes", &TThis::ForbidMaintenanceAttributeWrites)
        .Default(false);

    registrar.Parameter("enable_per_location_node_disposal", &TThis::EnablePerLocationNodeDisposal)
        .Default(true);

    registrar.Parameter("node_disposal_tick_period", &TThis::NodeDisposalTickPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("pending_restart_lease_timeout", &TThis::PendingRestartLeaseTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("reset_node_pending_restart_maintenance_period", &TThis::ResetNodePendingRestartMaintenancePeriod)
        .Default(TDuration::Seconds(2));

    registrar.Parameter("max_nodes_being_disposed", &TThis::MaxNodesBeingDisposed)
        .Default(10);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
