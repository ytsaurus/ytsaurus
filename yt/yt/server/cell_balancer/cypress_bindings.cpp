#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

static const std::string DefaultSpareNodeName = "spare";

////////////////////////////////////////////////////////////////////////////////

void TSysConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_bundle_controller", &TThis::DisableBundleController)
        .Default(false);
}

void TResourceQuota::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu", &TThis::Cpu)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("memory", &TThis::Memory)
        .GreaterThanOrEqual(0)
        .Default(0);

    // TODO(ifsmirnov): Finish migration started in YT-17749.
    registrar.Parameter("net_bytes", &TThis::Network)
        .GreaterThanOrEqual(0)
        .Default(0);
}

void TResourceLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_static_memory", &TThis::TabletStaticMemory)
        .GreaterThanOrEqual(0)
        .Default(0);
}

int TResourceQuota::Vcpu() const
{
    constexpr int VFactor = 1000;
    return static_cast<int>(Cpu * VFactor);
}

void THulkInstanceResources::Register(TRegistrar registrar)
{
    registrar.Parameter("vcpu", &TThis::Vcpu)
        .Default();
    registrar.Parameter("memory_mb", &TThis::MemoryMb)
        .Default();
    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Optional();
}

THulkInstanceResources& THulkInstanceResources::operator=(const NBundleControllerClient::TInstanceResources& resources)
{
    Vcpu = resources.Vcpu;
    MemoryMb = resources.Memory / 1_MB;
    NetworkBandwidth = resources.NetBytes;

    return *this;
}

void ConvertToInstanceResources(NBundleControllerClient::TInstanceResources& resources, const THulkInstanceResources& hulkResources)
{
    resources.Vcpu = hulkResources.Vcpu;
    resources.Memory = hulkResources.MemoryMb * 1_MB;
    resources.NetBytes = hulkResources.NetworkBandwidth;
}

void TBundleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_node_count", &TThis::TabletNodeCount)
        .Default(0);
    registrar.Parameter("rpc_proxy_count", &TThis::RpcProxyCount)
        .Default(0);
    registrar.Parameter("tablet_node_resource_guarantee", &TThis::TabletNodeResourceGuarantee)
        .DefaultNew();
    registrar.Parameter("rpc_proxy_resource_guarantee", &TThis::RpcProxyResourceGuarantee)
        .DefaultNew();
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();
    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();
    registrar.Parameter("medium_throughput_limits", &TThis::MediumThroughputLimits)
        .Default();
    registrar.Parameter("init_chaos_bundles", &TThis::InitChaosBundles)
        .Default(false);
    registrar.Parameter("additional_chaos_cell_count", &TThis::AdditionalChaosCellCount)
        .Default(0);
    registrar.Parameter("enable_drills_mode", &TThis::EnableDrillsMode)
        .Default(false);
    registrar.Parameter("redundant_rpc_proxy_data_center_count", &TThis::RedundantRpcProxyDataCenterCount)
        .Default();
    registrar.Parameter("forbidden_data_centers", &TThis::ForbiddenDataCenters)
        .Optional();
}

void TTabletCellStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("health", &TThis::Health)
        .Default();
    registrar.Parameter("decommissioned", &TThis::Decommissioned)
        .Default();
}

void TTabletCellPeer::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("state", &TThis::State)
        .Default();
    registrar.Parameter("last_seen_state", &TThis::LastSeenState)
        .Default();
    registrar.Parameter("last_seen_time", &TThis::LastSeenTime)
        .Default();
}

void TAbcInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Optional();
    registrar.Parameter("name", &TThis::Name)
        .Optional();
    registrar.Parameter("slug", &TThis::Slug)
        .Optional();
}

void TTabletCellInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("peers", &TThis::Peers)
        .Default();
}

void TGlobalCellRegistry::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_tag_range_begin", &TThis::CellTagRangeBegin)
        .GreaterThan(0)
        .IsRequired();
    registrar.Parameter("cell_tag_range_end", &TThis::CellTagRangeEnd)
        .GreaterThan(0)
        .IsRequired();
    registrar.Parameter("cell_tag_last", &TThis::CellTagLast)
        .Default();
    registrar.Parameter("cell_tags", &TThis::CellTags)
        .Default();
    registrar.Parameter("additional_cell_tags", &TThis::AdditionalCellTags)
        .Default();
}

void TCellTagInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("area", &TThis::Area)
        .Default();
    registrar.Parameter("cell_bundle", &TThis::CellBundle)
        .Default();
    registrar.Parameter("cell_id", &TThis::CellId)
        .Default();
}

void TDataCenterInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("yp_cluster", &TThis::YPCluster)
        .Default();
    registrar.Parameter("tablet_node_nanny_service", &TThis::TabletNodeNannyService)
        .Default();
    registrar.Parameter("rpc_proxy_nanny_service", &TThis::RpcProxyNannyService)
        .Default();
    registrar.Parameter("forbidden", &TThis::Forbidden)
        .Default(false);
}

void TBundleInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("health", &TThis::Health)
        .Default();
    registrar.Parameter("zone", &TThis::Zone)
        .Default();
    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter)
        .Default();
    registrar.Parameter("short_name", &TThis::ShortName)
        .Optional();
    registrar.Parameter("rpc_proxy_role", &TThis::RpcProxyRole)
        .Optional();
    registrar.Parameter("areas", &TThis::Areas)
        .Optional();
    registrar.Parameter("enable_bundle_controller", &TThis::EnableBundleController)
        .Default(false);
    registrar.Parameter("enable_instance_allocation", &TThis::EnableInstanceAllocation)
        .Default(true);
    registrar.Parameter("enable_tablet_cell_management", &TThis::EnableTabletCellManagement)
        .Default(true);
    registrar.Parameter("enable_node_tag_filter_management", &TThis::EnableNodeTagFilterManagement)
        .Default(true);
    registrar.Parameter("enable_tablet_node_dynamic_config", &TThis::EnableTabletNodeDynamicConfig)
        .Default(true);
    registrar.Parameter("enable_rpc_proxy_management", &TThis::EnableRpcProxyManagement)
        .Default(true);
    registrar.Parameter("enable_system_account_management", &TThis::EnableSystemAccountManagement)
        .Default(true);
    registrar.Parameter("enable_resource_limits_management", &TThis::EnableResourceLimitsManagement)
        .Default(true);
    registrar.Parameter("bundle_controller_target_config", &TThis::TargetConfig)
        .Default();
    registrar.Parameter("tablet_cell_ids", &TThis::TabletCellIds)
        .Default();
    registrar.Parameter("options", &TThis::Options)
        .DefaultNew();
    registrar.Parameter("resource_quota", &TThis::ResourceQuota)
        .Default();
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
    registrar.Parameter("system_account_quota_multiplier", &TThis::SystemAccountQuotaMultiplier)
        .GreaterThan(0)
        .Default(1.3);
    registrar.Parameter("folder_id", &TThis::FolderId)
        .Default();
    registrar.Parameter("abc", &TThis::Abc)
        .DefaultNew();

    registrar.Parameter("mute_tablet_cells_check", &TThis::MuteTabletCellsCheck)
        .Default(false);
    registrar.Parameter("mute_tablet_cell_snapshots_check", &TThis::MuteTabletCellSnapshotsCheck)
        .Default(false);
    registrar.Parameter("bc_hotfix", &TThis::BundleHotfix)
        .Default(false);
}

void TBundleArea::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("cell_count", &TThis::CellCount)
        .Default();
    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter)
        .Default();
}

void TChaosBundleInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();

    // The attribute name is misleading, ids are actually chaos cell ones.
    registrar.Parameter("tablet_cell_ids", &TThis::ChaosCellIds)
        .Default();

    registrar.Parameter("options", &TThis::Options)
        .DefaultNew();
    registrar.Parameter("areas", &TThis::Areas)
        .Default();
    registrar.Parameter("metadata_cell_ids", &TThis::MetadataCellIds)
        .Default();
}

void TZoneInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("yp_cluster", &TThis::DefaultYPCluster)
        .Default();
    registrar.Parameter("max_tablet_node_count", &TThis::MaxTabletNodeCount)
        .Default(10);
    registrar.Parameter("max_rpc_proxy_count", &TThis::MaxRpcProxyCount)
        .Default(10);
    registrar.Parameter("tablet_node_nanny_service", &TThis::DefaultTabletNodeNannyService)
        .Default();
    registrar.Parameter("rpc_proxy_nanny_service", &TThis::DefaultRpcProxyNannyService)
        .Default();
    registrar.Parameter("short_name", &TThis::ShortName)
        .Optional();

    registrar.Parameter("tablet_node_sizes", &TThis::TabletNodeSizes)
        .Default();
    registrar.Parameter("rpc_proxy_sizes", &TThis::RpcProxySizes)
        .Default();

    registrar.Parameter("spare_target_config", &TThis::SpareTargetConfig)
        .DefaultNew();
    registrar.Parameter("spare_bundle_name", &TThis::SpareBundleName)
        .Default(DefaultSpareNodeName);
    registrar.Parameter("disrupted_threshold_factor", &TThis::DisruptedThresholdFactor)
        .GreaterThan(0)
        .Default(1);

    registrar.Parameter("requires_minus_one_rack_guarantee", &TThis::RequiresMinusOneRackGuarantee)
        .Default(true);

    registrar.Parameter("redundant_data_center_count", &TThis::RedundantDataCenterCount)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("data_centers", &TThis::DataCenters)
        .Default();
}

void TAllocationRequestSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("yp_cluster", &TThis::YPCluster)
        .Default();
    registrar.Parameter("nanny_service", &TThis::NannyService)
        .Default();
    registrar.Parameter("resource_request", &TThis::ResourceRequest)
        .DefaultNew();
    registrar.Parameter("pod_id_template", &TThis::PodIdTemplate)
        .Default();
    registrar.Parameter("instance_role", &TThis::InstanceRole)
        .Default();
    registrar.Parameter("host_tag_filter", &TThis::HostTagFilter)
        .Optional();
}

void TAllocationRequestStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("state", &TThis::State)
        .Default();
    registrar.Parameter("node_id", &TThis::NodeId)
        .Default();
    registrar.Parameter("pod_id", &TThis::PodId)
        .Default();
}

void TAllocationRequest::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec)
        .DefaultNew();
    registrar.Parameter("status", &TThis::Status)
        .DefaultNew();
}

void TDeallocationRequestSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("yp_cluster", &TThis::YPCluster)
        .Default();
    registrar.Parameter("pod_id", &TThis::PodId)
        .Default();
    registrar.Parameter("instance_role", &TThis::InstanceRole)
        .Default();
}

void TDeallocationRequestStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("state", &TThis::State)
        .Default();
}

void TDeallocationRequest::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec)
        .DefaultNew();
    registrar.Parameter("status", &TThis::Status)
        .DefaultNew();
}

void TDrillsModeState::Register(TRegistrar registrar)
{
    registrar.Parameter("turning_on", &TThis::TurningOn)
        .Default();
    registrar.Parameter("turning_off", &TThis::TurningOff)
        .Default();
}

void TBundleControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("node_allocations", &TThis::NodeAllocations)
        .Default();
    registrar.Parameter("node_deallocations", &TThis::NodeDeallocations)
        .Default();
    registrar.Parameter("removing_cells", &TThis::RemovingCells)
        .Default();
    registrar.Parameter("proxy_allocations", &TThis::ProxyAllocations)
        .Default();
    registrar.Parameter("proxy_deallocations", &TThis::ProxyDeallocations)
        .Default();
    registrar.Parameter("bundle_node_assignments", &TThis::BundleNodeAssignments)
        .Default();
    registrar.Parameter("spare_node_assignments", &TThis::SpareNodeAssignments)
        .Default();
    registrar.Parameter("bundle_node_releasements", &TThis::BundleNodeReleasements)
        .Default();
    registrar.Parameter("spare_node_releasements", &TThis::SpareNodeReleasements)
        .Default();
    registrar.Parameter("drills_mode", &TThis::DrillsMode)
        .DefaultNew();
}

void TAllocationRequestState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
    registrar.Parameter("pod_id_template", &TThis::PodIdTemplate)
        .Default();
    registrar.Parameter("data_center", &TThis::DataCenter)
        .Optional();
}

void TDeallocationRequestState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
    registrar.Parameter("instance_name", &TThis::InstanceName)
        .Default();
    registrar.Parameter("strategy", &TThis::Strategy)
        .Default();
    registrar.Parameter("hulk_request_created", &TThis::HulkRequestCreated)
        .Default(false);
    registrar.Parameter("data_center", &TThis::DataCenter)
        .Optional();
}

void TRemovingTabletCellState::Register(TRegistrar registrar)
{
    registrar.Parameter("removed_time", &TThis::RemovedTime)
        .Default();
}

void TNodeTagFilterOperationState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
}

void TDrillsModeOperationState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
}

void TBundleControllerInstanceAnnotations::Register(TRegistrar registrar)
{
    registrar.Parameter("yp_cluster", &TThis::YPCluster)
        .Default();
    registrar.Parameter("nanny_service", &TThis::NannyService)
        .Default();
    registrar.Parameter("allocated_for_bundle", &TThis::AllocatedForBundle)
        .Default();
    registrar.Parameter("allocated", &TThis::Allocated)
        .Default(false);
    registrar.Parameter("resources", &TThis::Resource)
        .DefaultNew();
    registrar.Parameter("deallocated_at", &TThis::DeallocatedAt)
        .Optional();
    registrar.Parameter("deallocation_strategy", &TThis::DeallocationStrategy)
        .Default();
    registrar.Parameter("data_center", &TThis::DataCenter)
        .Optional();
}

void TCypressAnnotations::Register(TRegistrar registrar)
{
    registrar.Parameter("pod_id", &TThis::PodId)
        .Optional();
}

void TTabletSlot::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_cell_bundle", &TThis::TabletCellBundle)
        .Default();
    registrar.Parameter("cell_id", &TThis::CellId)
        .Default();
    registrar.Parameter("peer_id", &TThis::PeerId)
        .Default();
    registrar.Parameter("state", &TThis::State)
        .Default();
}

void TCmsMaintenanceRequest::Register(TRegistrar /*registrar*/)
{ }

void TMemoryCategory::Register(TRegistrar registrar)
{
    registrar.Parameter("used", &TThis::Used)
        .Default(0);
    registrar.Parameter("limit", &TThis::Limit)
        .Default(0);
}

void TTabletNodeMemoryStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_dynamic", &TThis::TabletDynamic)
        .DefaultNew();
    registrar.Parameter("tablet_static", &TThis::TabletStatic)
        .DefaultNew();
}

void TTabletNodeStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("memory", &TThis::Memory)
        .DefaultNew();
}

void TInstanceInfoBase::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_controller_annotations", &TThis::BundleControllerAnnotations)
        .DefaultNew();
    registrar.Parameter("annotations", &TThis::CypressAnnotations)
        .DefaultNew();
    registrar.Parameter("cms_maintenance_requests", &TThis::CmsMaintenanceRequests)
        .Default();
}

void TTabletNodeInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("banned", &TThis::Banned)
        .Default();
    registrar.Parameter("decommissioned", &TThis::Decommissioned)
        .Default();
    registrar.Parameter("disable_tablet_cells", &TThis::DisableTabletCells)
        .Default(false);
    registrar.Parameter("enable_bundle_balancer", &TThis::EnableBundleBalancer)
        .Optional();
    registrar.Parameter("host", &TThis::Host)
        .Default();
    registrar.Parameter("state", &TThis::State)
        .Default();
    registrar.Parameter("tags", &TThis::Tags)
        .Default();
    registrar.Parameter("user_tags", &TThis::UserTags)
        .Default();
    registrar.Parameter("tablet_slots", &TThis::TabletSlots)
        .Default();
    registrar.Parameter("last_seen_time", &TThis::LastSeenTime)
        .Default();
    registrar.Parameter("rack", &TThis::Rack)
        .Default();
    registrar.Parameter("statistics", &TThis::Statistics)
        .DefaultNew();
}

void TMediumThroughputLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_byte_rate", &TThis::WriteByteRate)
        .Default();

    registrar.Parameter("read_byte_rate", &TThis::ReadByteRate)
        .Default();
}

void TBundleDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .Default();

    registrar.Parameter("medium_throughput_limits", &TThis::MediumThroughputLimits)
        .Default();
}

void TRpcProxyAlive::Register(TRegistrar /*registrar*/)
{ }

void TRpcProxyInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("banned", &TThis::Banned)
        .Default();
    registrar.Parameter("role", &TThis::Role)
        .Default();
    registrar.Parameter("modification_time", &TThis::ModificationTime)
        .Default();

    registrar.Parameter("alive", &TThis::Alive)
        .Default();
}

void TAccountResources::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_count", &TThis::ChunkCount)
        .Default();

    registrar.Parameter("disk_space_per_medium", &TThis::DiskSpacePerMedium)
        .Default();

    registrar.Parameter("node_count", &TThis::NodeCount)
        .Default();
}

void TSystemAccount::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    registrar.Parameter("resource_usage", &TThis::ResourceUsage)
        .DefaultNew();
}

void TBundleSystemOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("changelog_account", &TThis::ChangelogAccount)
        .Default();

    registrar.Parameter("changelog_primary_medium", &TThis::ChangelogPrimaryMedium)
        .Default();

    registrar.Parameter("snapshot_account", &TThis::SnapshotAccount)
        .Default();

    registrar.Parameter("snapshot_primary_medium", &TThis::SnapshotPrimaryMedium)
        .Default();

    registrar.Parameter("peer_count", &TThis::PeerCount)
        .GreaterThan(0)
        .Default(1);

    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
