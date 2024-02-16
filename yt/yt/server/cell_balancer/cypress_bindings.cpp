#include "cypress_bindings.h"

#include <yt/yt/server/node/cluster_node/config.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultSpareNodeName = "spare";

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

    if (resources.Net) {
        NetworkBandwidth = *resources.Net / 8;
    }

    return *this;
}

void ConvertToInstanceResources(NBundleControllerClient::TInstanceResources& resources, const THulkInstanceResources& hulkResources) {
    resources.Vcpu = hulkResources.Vcpu;
    resources.Memory = hulkResources.MemoryMb * 1_MB;

    if (hulkResources.NetworkBandwidth) {
        resources.Net = *hulkResources.NetworkBandwidth * 8;
    }
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
}

void TTabletCellInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "peers", &TThis::Peers)
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
    RegisterAttribute(registrar, "health", &TThis::Health)
        .Default();
    RegisterAttribute(registrar, "zone", &TThis::Zone)
        .Default();
    RegisterAttribute(registrar, "node_tag_filter", &TThis::NodeTagFilter)
        .Default();
    RegisterAttribute(registrar, "short_name", &TThis::ShortName)
        .Optional();
    RegisterAttribute(registrar, "rpc_proxy_role", &TThis::RpcProxyRole)
        .Optional();
    RegisterAttribute(registrar, "forbidden_data_centers", &TThis::ForbiddenDataCenters)
        .Optional();
    RegisterAttribute(registrar, "enable_bundle_controller", &TThis::EnableBundleController)
        .Default(false);
    RegisterAttribute(registrar, "enable_instance_allocation", &TThis::EnableInstanceAllocation)
        .Default(true);
    RegisterAttribute(registrar, "enable_tablet_cell_management", &TThis::EnableTabletCellManagement)
        .Default(true);
    RegisterAttribute(registrar, "enable_node_tag_filter_management", &TThis::EnableNodeTagFilterManagement)
        .Default(true);
    RegisterAttribute(registrar, "enable_tablet_node_dynamic_config", &TThis::EnableTabletNodeDynamicConfig)
        .Default(true);
    RegisterAttribute(registrar, "enable_rpc_proxy_management", &TThis::EnableRpcProxyManagement)
        .Default(true);
    RegisterAttribute(registrar, "enable_system_account_management", &TThis::EnableSystemAccountManagement)
        .Default(true);
    RegisterAttribute(registrar, "enable_resource_limits_management", &TThis::EnableResourceLimitsManagement)
        .Default(true);
    RegisterAttribute(registrar, "bundle_controller_target_config", &TThis::TargetConfig)
        .Default();
    RegisterAttribute(registrar, "tablet_cell_ids", &TThis::TabletCellIds)
        .Default();
    RegisterAttribute(registrar, "options", &TThis::Options)
        .DefaultNew();
    RegisterAttribute(registrar, "resource_quota", &TThis::ResourceQuota)
        .Default();
    RegisterAttribute(registrar, "resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
    RegisterAttribute(registrar, "system_account_quota_multiplier", &TThis::SystemAccountQuotaMultiplier)
        .GreaterThan(0)
        .Default(1.3);
}

void TZoneInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "yp_cluster", &TThis::DefaultYPCluster)
        .Default();
    RegisterAttribute(registrar, "max_tablet_node_count", &TThis::MaxTabletNodeCount)
        .Default(10);
    RegisterAttribute(registrar, "max_rpc_proxy_count", &TThis::MaxRpcProxyCount)
        .Default(10);
    RegisterAttribute(registrar, "tablet_node_nanny_service", &TThis::DefaultTabletNodeNannyService)
        .Default();
    RegisterAttribute(registrar, "rpc_proxy_nanny_service", &TThis::DefaultRpcProxyNannyService)
        .Default();
    RegisterAttribute(registrar, "short_name", &TThis::ShortName)
        .Optional();

    RegisterAttribute(registrar, "tablet_node_sizes", &TThis::TabletNodeSizes)
        .Default();
    RegisterAttribute(registrar, "rpc_proxy_sizes", &TThis::RpcProxySizes)
        .Default();

    RegisterAttribute(registrar, "spare_target_config", &TThis::SpareTargetConfig)
        .DefaultNew();
    RegisterAttribute(registrar, "spare_bundle_name", &TThis::SpareBundleName)
        .Default(DefaultSpareNodeName);
    RegisterAttribute(registrar, "disrupted_threshold_factor", &TThis::DisruptedThresholdFactor)
        .GreaterThan(0)
        .Default(1);

    RegisterAttribute(registrar, "requires_minus_one_rack_guarantee", &TThis::RequiresMinusOneRackGuarantee)
        .Default(true);

    RegisterAttribute(registrar, "redundant_data_center_count", &TThis::RedundantDataCenterCount)
        .GreaterThanOrEqual(0)
        .Default(0);

    RegisterAttribute(registrar, "data_centers", &TThis::DataCenters)
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

void TBundleControllerState::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "node_allocations", &TThis::NodeAllocations)
        .Default();
    RegisterAttribute(registrar, "node_deallocations", &TThis::NodeDeallocations)
        .Default();
    RegisterAttribute(registrar, "removing_cells", &TThis::RemovingCells)
        .Default();
    RegisterAttribute(registrar, "proxy_allocations", &TThis::ProxyAllocations)
        .Default();
    RegisterAttribute(registrar, "proxy_deallocations", &TThis::ProxyDeallocations)
        .Default();
    RegisterAttribute(registrar, "bundle_node_assignments", &TThis::BundleNodeAssignments)
        .Default();
    RegisterAttribute(registrar, "spare_node_assignments", &TThis::SpareNodeAssignments)
        .Default();
    RegisterAttribute(registrar, "bundle_node_releasements", &TThis::BundleNodeReleasements)
        .Default();
    RegisterAttribute(registrar, "spare_node_releasements", &TThis::SpareNodeReleasements)
        .Default();
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

void TInstanceAnnotations::Register(TRegistrar registrar)
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

void TTabletNodeInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "banned", &TThis::Banned)
        .Default();
    RegisterAttribute(registrar, "decommissioned", &TThis::Decommissioned)
        .Default();
    RegisterAttribute(registrar, "disable_tablet_cells", &TThis::DisableTabletCells)
        .Default(false);
    RegisterAttribute(registrar, "enable_bundle_balancer", &TThis::EnableBundleBalancer)
        .Optional();
    RegisterAttribute(registrar, "host", &TThis::Host)
        .Default();
    RegisterAttribute(registrar, "state", &TThis::State)
        .Default();
    RegisterAttribute(registrar, "tags", &TThis::Tags)
        .Default();
    RegisterAttribute(registrar, "user_tags", &TThis::UserTags)
        .Default();
    RegisterAttribute(registrar, "bundle_controller_annotations", &TThis::Annotations)
        .DefaultNew();
    RegisterAttribute(registrar, "tablet_slots", &TThis::TabletSlots)
        .Default();
    RegisterAttribute(registrar, "cms_maintenance_requests", &TThis::CmsMaintenanceRequests)
        .Default();
    RegisterAttribute(registrar, "last_seen_time", &TThis::LastSeenTime)
        .Default();
    RegisterAttribute(registrar, "rack", &TThis::Rack)
        .Default();
    RegisterAttribute(registrar, "statistics", &TThis::Statistics)
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
    RegisterAttribute(registrar, "banned", &TThis::Banned)
        .Default();
    RegisterAttribute(registrar, "role", &TThis::Role)
        .Default();
    RegisterAttribute(registrar, "bundle_controller_annotations", &TThis::Annotations)
        .DefaultNew();
    RegisterAttribute(registrar, "cms_maintenance_requests", &TThis::CmsMaintenanceRequests)
        .Default();
    RegisterAttribute(registrar, "modification_time", &TThis::ModificationTime)
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
    RegisterAttribute(registrar, "resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    RegisterAttribute(registrar, "resource_usage", &TThis::ResourceUsage)
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
