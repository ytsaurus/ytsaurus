#include "node.h"

#include "resource_capacities.h"
#include "topology_zone.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TResourceBase::TResourceBase(
    const TResourceCapacities& totalCapacities,
    const TResourceCapacities& allocatedCapacities)
    : TotalCapacities_(totalCapacities)
    , AllocatedCapacities_(allocatedCapacities)
{ }

const TResourceCapacities& TResourceBase::GetTotalCapacities() const
{
    return TotalCapacities_;
}

const TResourceCapacities& TResourceBase::GetAllocatedCapacities() const
{
    return AllocatedCapacities_;
}

TResourceCapacities TResourceBase::GetFreeCapacities() const
{
    auto freeCapacities = TotalCapacities_;
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        freeCapacities[index] = (freeCapacities[index] >= AllocatedCapacities_[index])
            ? freeCapacities[index] - AllocatedCapacities_[index]
            : 0;
    }
    return freeCapacities;
}

////////////////////////////////////////////////////////////////////////////////

bool THomogeneousResource::CanAllocate(const TResourceCapacities& capacities) const
{
    return Dominates(TotalCapacities_, AllocatedCapacities_ + capacities);
}

////////////////////////////////////////////////////////////////////////////////

bool THomogeneousResource::TryAllocate(const TResourceCapacities& capacities)
{
    if (!Dominates(TotalCapacities_, AllocatedCapacities_ + capacities)) {
        return false;
    }

    AllocatedCapacities_ += capacities;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TDiskResource::TDiskResource(
    TString storageClass,
    const TDiskVolumePolicyList& supportedPolicies,
    const TResourceCapacities& totalCapacities,
    bool used,
    bool usedExclusively,
    const TResourceCapacities& allocatedCapacities)
    : TResourceBase(totalCapacities, allocatedCapacities)
    , StorageClass_(std::move(storageClass))
    , SupportedPolicies_(supportedPolicies)
    , Used_(used)
    , UsedExclusively_(usedExclusively)
{ }

const TString& TDiskResource::GetStorageClass() const
{
    return StorageClass_;
}

bool TDiskResource::TryAllocate(
    bool exclusive,
    const TString& storageClass,
    NClient::NApi::NProto::EDiskVolumePolicy policy,
    const TResourceCapacities& capacities)
{
    if (UsedExclusively_) {
        return false;
    }
    if (Used_ && exclusive) {
        return false;
    }
    if (StorageClass_ != storageClass) {
        return false;
    }
    if (std::find(SupportedPolicies_.begin(), SupportedPolicies_.end(), policy) == SupportedPolicies_.end()) {
        return false;
    }
    if (!Dominates(TotalCapacities_, AllocatedCapacities_ + capacities)) {
        return false;
    }

    AllocatedCapacities_ += capacities;
    Used_ = true;
    if (exclusive) {
        UsedExclusively_ = true;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TGpuResource::TGpuResource(
    const TResourceCapacities& totalCapacities,
    const TResourceCapacities& allocatedCapacities,
    TString model,
    ui64 totalMemory)
    : TResourceBase(totalCapacities, allocatedCapacities)
    , Model_(std::move(model))
    , TotalMemory_(totalMemory)
{ }

const TString& TGpuResource::GetModel() const
{
    return Model_;
}

ui64 TGpuResource::GetTotalMemory() const
{
    return TotalMemory_;
}

bool TGpuResource::TryAllocate(const TResourceCapacities& capacities)
{
    if (!Dominates(TotalCapacities_, AllocatedCapacities_ + capacities)) {
        return false;
    }

    AllocatedCapacities_ += capacities;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    NObjects::EHfsmState hfsmState,
    bool hasUnknownPods,
    const NObjects::TNodeAlerts& alerts,
    NClient::NApi::NProto::TNodeStatus_TMaintenance maintenance,
    NClient::NApi::NProto::TNodeSpec spec)
    : TObject(std::move(id), std::move(labels))
    , HfsmState_(hfsmState)
    , HasUnknownPods_(hasUnknownPods)
    , Alerts_(alerts)
    , Maintenance_(std::move(maintenance))
    , Spec_(std::move(spec))
{ }

bool TNode::CanAllocateAntiaffinityVacancies(const TPod* pod) const
{
    for (auto* zone : TopologyZones_) {
        if (!zone->CanAllocateAntiaffinityVacancies(pod)) {
            return false;
        }
    }
    return true;
}

void TNode::AllocateAntiaffinityVacancies(const TPod* pod)
{
    for (auto* zone : TopologyZones_) {
        zone->AllocateAntiaffinityVacancies(pod);
    }
}

bool TNode::IsSchedulable() const
{
    return
        HfsmState_ == NObjects::EHfsmState::Up &&
        !HasUnknownPods_ &&
        Alerts_.empty();
}

bool TNode::HasIP6SubnetInVlan(const TString& vlanId) const
{
    for (const auto& subnet : Spec().ip6_subnets()) {
        if (vlanId == subnet.vlan_id()) {
            return true;
        }
    }
    return false;
}

ui64 TNode::GetDiskResourceTotalFreeCapacity(const TString& storageClass) const
{
    ui64 result = 0;
    for (const auto& resource : DiskResources_) {
        if (resource.GetStorageClass() == storageClass) {
            result += GetDiskCapacity(resource.GetFreeCapacities());
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
