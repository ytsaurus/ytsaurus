#include "node.h"
#include "topology_zone.h"
#include "helpers.h"

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

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
        YT_VERIFY(freeCapacities[index] >= AllocatedCapacities_[index]);
        freeCapacities[index] -= AllocatedCapacities_[index];
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

TNode::TNode(
    TObjectId id,
    TYsonString labels,
    EHfsmState hfsmState,
    ENodeMaintenanceState maintenanceState,
    bool hasUnknownPods,
    NClient::NApi::NProto::TNodeSpec spec)
    : TObject(std::move(id), std::move(labels))
    , HfsmState_(hfsmState)
    , MaintenanceState_(maintenanceState)
    , HasUnknownPods_(hasUnknownPods)
    , Spec_(std::move(spec))
{ }

bool TNode::CanAcquireAntiaffinityVacancies(const TPod* pod) const
{
    for (auto* zone : TopologyZones_) {
        if (!zone->CanAcquireAntiaffinityVacancy(pod)) {
            return false;
        }
    }
    return true;
}

void TNode::AcquireAntiaffinityVacancies(const TPod* pod)
{
    for (auto* zone : TopologyZones_) {
        zone->AcquireAntiaffinityVacancy(pod);
    }
}

bool TNode::IsSchedulable() const
{
    return
        HfsmState_ == EHfsmState::Up &&
        !HasUnknownPods_;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
