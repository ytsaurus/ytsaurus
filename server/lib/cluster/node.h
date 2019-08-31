#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TResourceBase
{
public:
    TResourceBase() = default;
    TResourceBase(
        const TResourceCapacities& totalCapacities,
        const TResourceCapacities& allocatedCapacities);
    TResourceBase(const TResourceBase&) = default;

    const TResourceCapacities& GetTotalCapacities() const;
    const TResourceCapacities& GetAllocatedCapacities() const;
    TResourceCapacities GetFreeCapacities() const;

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

    bool CanAllocate(const TResourceCapacities& capacities) const;
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

    const TString& GetStorageClass() const;

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

class TGpuResource
    : public TResourceBase
{
public:
    TGpuResource() = default;
    TGpuResource(
        const TResourceCapacities& totalCapacities,
        const TResourceCapacities& allocatedCapacities,
        TString model,
        ui64 totalMemory);
    TGpuResource(const TGpuResource&) = default;

    const TString& GetModel() const;
    ui64 GetTotalMemory() const;

    bool TryAllocate(const TResourceCapacities& capacities);

private:
    TString Model_;
    ui64 TotalMemory_;
};

////////////////////////////////////////////////////////////////////////////////

class TNode
    : public TObject
    , public NYT::TRefTracked<TNode>
{
public:
    static constexpr EObjectType Type = EObjectType::Node;

    TNode(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        NObjects::EHfsmState hfsmState,
        NObjects::ENodeMaintenanceState maintenanceState,
        bool hasUnknownPods,
        NClient::NApi::NProto::TNodeSpec spec);

    DEFINE_BYVAL_RO_PROPERTY(NObjects::EHfsmState, HfsmState);
    DEFINE_BYVAL_RO_PROPERTY(NObjects::ENodeMaintenanceState, MaintenanceState);
    DEFINE_BYVAL_RO_PROPERTY(bool, HasUnknownPods);
    DEFINE_BYREF_RW_PROPERTY(NClient::NApi::NProto::TNodeSpec, Spec);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TTopologyZone*>, TopologyZones);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, Pods);

    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, CpuResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, MemoryResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, NetworkResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, SlotResource);
    using TDiskResources = SmallVector<TDiskResource, NObjects::TypicalDiskResourceCountPerNode>;
    DEFINE_BYREF_RW_PROPERTY(TDiskResources, DiskResources);
    using TGpuResources = SmallVector<TGpuResource, NObjects::TypicalGpuResourceCountPerNode>;
    DEFINE_BYREF_RW_PROPERTY(TGpuResources, GpuResources);

    bool CanAcquireAntiaffinityVacancies(const TPod* pod) const;
    void AcquireAntiaffinityVacancies(const TPod* pod);

    bool IsSchedulable() const;
    bool HasIP6SubnetInVlan(const TString& vlanId) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
