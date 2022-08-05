#include "cypress_bindings.h"

#include <yt/yt/server/node/cluster_node/config.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

void TCpuLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_thread_pool_size", &TThis::WriteThreadPoolSize)
        .GreaterThan(0)
        .Default(5);
}

void TInstanceResources::Register(TRegistrar registrar)
{
    registrar.Parameter("vcpu", &TThis::VCpu)
        .GreaterThan(0)
        .Default(18000);
    registrar.Parameter("memory", &TThis::Memory)
        .GreaterThan(0)
        .Default(120_GB);
}

void THulkInstanceResources::Register(TRegistrar registrar)
{
    registrar.Parameter("vcpu", &TThis::VCpu)
        .Default();
    registrar.Parameter("memory_mb", &TThis::MemoryMb)
        .Default();
}

THulkInstanceResources& THulkInstanceResources::operator=(const TInstanceResources& resources)
{
    VCpu = resources.VCpu;
    MemoryMb = resources.Memory / 1_MB;

    return *this;
}

void TBundleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_node_count", &TThis::TabletNodeCount)
        .Default(0);
    registrar.Parameter("tablet_node_resource_guarantee", &TThis::TabletNodeResourceGuarantee)
        .DefaultNew();
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();
    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
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
    RegisterAttribute(registrar, "tablet_cell_bundle", &TThis::TabletCellBundle)
        .Default();
    RegisterAttribute(registrar, "tablet_cell_life_stage", &TThis::TabletCellLifeStage)
        .Default();
    RegisterAttribute(registrar, "tablet_count", &TThis::TabletCount)
        .Default();
    RegisterAttribute(registrar, "status", &TThis::Status)
        .DefaultNew();
    RegisterAttribute(registrar, "peers", &TThis::Peers)
        .Default();
}

void TBundleInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "health", &TThis::Health)
        .Default();
    RegisterAttribute(registrar, "zone", &TThis::Zone)
        .Default();
    RegisterAttribute(registrar, "node_tag_filter", &TThis::NodeTagFilter)
        .Default();
    RegisterAttribute(registrar, "enable_bundle_controller", &TThis::EnableBundleController)
        .Default(false);
    RegisterAttribute(registrar, "enable_tablet_cell_management", &TThis::EnableTabletCellManagement)
        .Default(false);
    RegisterAttribute(registrar, "enable_node_tag_filter_management", &TThis::EnableNodeTagFilterManagement)
        .Default(false);
    RegisterAttribute(registrar, "enable_tablet_node_dynamic_config", &TThis::EnableTabletNodeDynamicConfig)
        .Default(false);
    RegisterAttribute(registrar, "bundle_controller_target_config", &TThis::TargetConfig)
        .DefaultNew();
    RegisterAttribute(registrar, "bundle_controller_actual_config", &TThis::ActualConfig)
        .DefaultNew();
    RegisterAttribute(registrar, "tablet_cell_ids", &TThis::TabletCellIds)
        .Default();
}

void TZoneInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "yp_cluster", &TThis::YPCluster)
        .Default();

    RegisterAttribute(registrar, "max_tablet_node_count", &TThis::MaxTabletNodeCount)
        .Default(10);

    RegisterAttribute(registrar, "nanny_service", &TThis::NannyService)
        .Default();

    RegisterAttribute(registrar, "spare_target_config", &TThis::SpareTargetConfig)
        .DefaultNew();

    RegisterAttribute(registrar, "disrupted_threshold_factor", &TThis::DisruptedThresholdFactor)
        .GreaterThan(0)
        .Default(1);
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
    RegisterAttribute(registrar, "allocation", &TThis::Allocations)
        .Default();
    RegisterAttribute(registrar, "deallocations", &TThis::Deallocations)
        .Default();
    RegisterAttribute(registrar, "removing_cells", &TThis::RemovingCells)
        .Default();
}

void TAllocationRequestState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
}

void TDeallocationRequestState::Register(TRegistrar registrar)
{
    registrar.Parameter("creation_time", &TThis::CreationTime)
        .Default();
    registrar.Parameter("node_name", &TThis::NodeName)
        .Default();
    registrar.Parameter("hulk_request_created", &TThis::HulkRequestCreated)
        .Default(false);
}

void TRemovingTabletCellInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("removed_time", &TThis::RemovedTime)
        .Default();
}

void TTabletNodeAnnotationsInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "yp_cluster", &TThis::YPCluster)
        .Default();
    RegisterAttribute(registrar, "nanny_service", &TThis::NannyService)
        .Default();
    RegisterAttribute(registrar, "allocated_for_bundle", &TThis::AllocatedForBundle)
        .Default();
    RegisterAttribute(registrar, "allocated", &TThis::Allocated)
        .Default(false);
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

void TTabletNodeInfo::Register(TRegistrar registrar)
{
    RegisterAttribute(registrar, "banned", &TThis::Banned)
        .Default();
    RegisterAttribute(registrar, "decommissioned", &TThis::Decommissioned)
        .Default();
    RegisterAttribute(registrar, "disable_tablet_cells", &TThis::DisableTabletCells)
        .Default(false);
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
}

void TBundleDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
