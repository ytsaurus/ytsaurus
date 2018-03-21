#include "node.h"
#include "topology_zone.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TResourceStatus::TResourceStatus()
    : TotalCapacity_(0)
    , AllocatedCapacity_(0)
{ }

TResourceStatus::TResourceStatus(ui64 totalCapacity, ui64 allocatedCapacity)
    : TotalCapacity_(totalCapacity)
    , AllocatedCapacity_(allocatedCapacity)
{ }

ui64 TResourceStatus::GetTotalCapacity() const
{
    return TotalCapacity_;
}

ui64 TResourceStatus::GetAllocatedCapacity() const
{
    return AllocatedCapacity_;
}

bool TResourceStatus::CanAllocateCapacity(ui64 capacity) const
{
    return AllocatedCapacity_ + capacity <= TotalCapacity_;
}

bool TResourceStatus::TryAllocateCapacity(ui64 capacity)
{
    if (CanAllocateCapacity(capacity)) {
        AllocatedCapacity_ += capacity;
        return true;
    } else {
        return false;
    }
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

TResourceStatus* TNode::GetHomegeneousResourceStatus(EResourceKind kind)
{
    switch (kind) {
        case EResourceKind::Cpu:
            return &CpuStatus_;
        case EResourceKind::Memory:
            return &MemoryStatus_;
        default:
            Y_UNREACHABLE();
    }
}

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

void TNode::ReleaseAntiaffinityVacancies(const TPod* pod)
{
    for (auto* zone : TopologyZones_) {
        zone->ReleaseAntiaffinityVacancy(pod);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

