#include "node.h"
#include "topology_zone.h"
#include "helpers.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TResourceBase::TResourceBase(
    const TResourceCapacities& totalCapacities,
    const TResourceCapacities& allocatedCapacities)
    : TotalCapacities_(totalCapacities)
    , AllocatedCapacities_(allocatedCapacities)
{ }

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

bool TDiskResource::TryAllocate(
    bool exclusive,
    const TString& storageClass,
    NClient::NApi::NProto::EDiskVolumePolicy policy,
    const TResourceCapacities& capacities)
{
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
    const TObjectId& id,
    TYsonString labels,
    std::vector<TTopologyZone*> topologyZones,
    EHfsmState hfsmState,
    ENodeMaintenanceState maintenanceState)
    : TObject(id, std::move(labels))
    , TopologyZones_(std::move(topologyZones))
    , HfsmState_(hfsmState)
    , MaintenanceState_(maintenanceState)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

