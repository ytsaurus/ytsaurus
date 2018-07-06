#pragma once

#include "object.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>
#include <yp/client/api/proto/data_model.pb.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceBase
{
public:
    TResourceBase() = default;
    TResourceBase(
        const TResourceCapacities& totalCapacities,
        const TResourceCapacities& allocatedCapacities);
    TResourceBase(const TResourceBase&) = default;

protected:
    TResourceCapacities TotalCapacities_ = {};
    TResourceCapacities AllocatedCapacities_ = {};
};

////////////////////////////////////////////////////////////////////////////////

class THomogeneousResource
    : public TResourceBase
{
public:
    using TResourceBase::TResourceBase;
    THomogeneousResource() = default;
    THomogeneousResource(const THomogeneousResource&) = default;

    bool TryAllocate(const TResourceCapacities& capacities);
};

////////////////////////////////////////////////////////////////////////////////

using TDiskVolumePolicyList = SmallVector<NClient::NApi::NProto::EDiskVolumePolicy, 4>;

class TDiskResource
    : public TResourceBase
{
public:
    TDiskResource() = default;
    TDiskResource(
        TString storageClass,
        const TDiskVolumePolicyList& supportedPolicies,
        const TResourceCapacities& totalCapacities,
        bool used,
        bool usedExclusively,
        const TResourceCapacities& allocatedCapacities);
    TDiskResource(const TDiskResource&) = default;

    bool TryAllocate(
        bool exclusive,
        const TString& storageClass,
        NClient::NApi::NProto::EDiskVolumePolicy policy,
        const TResourceCapacities& capacities);

private:
    TString StorageClass_;
    TDiskVolumePolicyList SupportedPolicies_;
    bool Used_ = false;
    bool UsedExclusively_ = false;
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
        NObjects::ENodeMaintenanceState maintenanceState,
        NClient::NApi::NProto::TNodeSpec spec);

    bool CanAcquireAntiaffinityVacancies(const TPod* pod) const;
    void AcquireAntiaffinityVacancies(const TPod* pod);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TTopologyZone*>, TopologyZones);
    DEFINE_BYVAL_RO_PROPERTY(NObjects::EHfsmState, HfsmState);
    DEFINE_BYVAL_RO_PROPERTY(NObjects::ENodeMaintenanceState, MaintenanceState);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, Pods);
    DEFINE_BYREF_RW_PROPERTY(NClient::NApi::NProto::TNodeSpec, Spec);

    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, CpuResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, MemoryResource);
    using TDiskResources = SmallVector<TDiskResource, NObjects::TypicalDiskResourceCountPerNode>;
    DEFINE_BYREF_RW_PROPERTY(TDiskResources, DiskResources);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
