#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yp/server/objects/proto/objects.pb.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceStatus
{
public:
    TResourceStatus();
    TResourceStatus(
        ui64 totalCapacity,
        ui64 allocatedCapacity);

    ui64 GetTotalCapacity() const;
    ui64 GetAllocatedCapacity() const;
    bool CanAllocateCapacity(ui64 capacity) const;
    bool TryAllocateCapacity(ui64 capacity);

private:
    ui64 TotalCapacity_;
    ui64 AllocatedCapacity_;
};

////////////////////////////////////////////////////////////////////////////////

class TNode
    : public TObject
    , public NYT::TRefTracked<TNode>
{
public:
    TNode(
        const TObjectId& id,
        NYT::NYson::TYsonString labels,
        std::vector<TTopologyZone*> topologyZones,
        NObjects::EHfsmState hfsmState,
        NObjects::ENodeMaintenanceState maintenanceState);

    TResourceStatus* GetHomegeneousResourceStatus(EResourceKind kind);

    bool CanAcquireAntiaffinityVacancies(const TPod* pod) const;
    void AcquireAntiaffinityVacancies(const TPod* pod);
    void ReleaseAntiaffinityVacancies(const TPod* pod);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TTopologyZone*>, TopologyZones);
    DEFINE_BYVAL_RO_PROPERTY(NObjects::EHfsmState, HfsmState);
    DEFINE_BYVAL_RO_PROPERTY(NObjects::ENodeMaintenanceState, MaintenanceState);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, Pods);

private:
    TResourceStatus CpuStatus_;
    TResourceStatus MemoryStatus_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
