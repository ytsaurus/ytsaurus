#pragma once

#include "public.h"

#include <yt/yt/client/bundle_controller_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IAllocatorAdapter
    : public TRefCounted
{
public:
    virtual bool IsNewAllocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& /*dataCenterName*/,
        const TSchedulerInputState& /*input*/) = 0;

    virtual bool IsNewDeallocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& dataCenterName,
        const TSchedulerInputState& /*input*/) = 0;

    virtual bool IsInstanceCountLimitReached(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const = 0;

    virtual bool IsDataCenterDisrupted(const TDataCenterDisruptedState& dcState) = 0;

    virtual int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo) const = 0;

    virtual int GetInstanceRole() const = 0;

    virtual const NBundleControllerClient::TInstanceResourcesPtr& GetResourceGuarantee(
        const TBundleInfoPtr& bundleInfo) const = 0;

    virtual const std::optional<std::string> GetHostTagFilter(
        const TBundleInfoPtr& /*bundleInfo*/,
        const TSchedulerInputState& /*input*/) const = 0;

    virtual const std::string& GetInstanceType() = 0;

    virtual const std::string& GetHumanReadableInstanceType() const = 0;

    virtual TIndexedEntries<TAllocationRequestState>& AllocationsState() const = 0;

    virtual TIndexedEntries<TDeallocationRequestState>& DeallocationsState() const = 0;

    virtual TInstanceInfoBasePtr FindInstanceInfo(
        const std::string& instanceName,
        const TSchedulerInputState& input) = 0;

    virtual TInstanceInfoBasePtr GetInstanceInfo(
        const std::string& instanceName,
        const TSchedulerInputState& input) = 0;

    virtual void SetInstanceAnnotations(
        const std::string& instanceName,
        TBundleControllerInstanceAnnotationsPtr bundleControllerAnnotations,
        TSchedulerMutations* mutations) = 0;

    virtual bool IsInstanceReadyToBeDeallocated(
        const std::string& /*instanceName*/,
        const std::string& /*deallocationId*/,
        TDuration /*deallocationAge*/,
        const std::string& /*bundleName*/,
        const TSchedulerInputState& /*input*/,
        TSchedulerMutations* /*mutations*/) const = 0;

    virtual std::string GetNannyService(const TDataCenterInfoPtr& dataCenterInfo) const = 0;

    virtual bool EnsureAllocatedInstanceTagsSet(
        const std::string& proxyName,
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const = 0;

    virtual bool EnsureDeallocatedInstanceTagsSet(
        const std::string& bundleName,
        const std::string& proxyName,
        const std::string& strategy,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) = 0;

    virtual void SetDefaultSpareAttributes(const std::string& proxyName, TSchedulerMutations* mutations) const = 0;

    virtual const THashSet<std::string>& GetAliveInstances(const std::string& dataCenterName) const = 0;
    virtual std::vector<std::string> GetOfflineInstances(
        const TSchedulerInputState& input,
        const std::string& dataCenterName) const = 0;

    virtual const std::vector<std::string>& GetInstances(const std::string& dataCenterName) const = 0;

    virtual std::vector<std::string> PickInstancesToDeallocate(
        int proxyCountToRemove,
        const std::string& dataCenterName,
        const TBundleInfoPtr& bundleInfo,
        const TSchedulerInputState& input) const = 0;

    // Returns the size map for this instance type (TabletNodeSizes or RpcProxySizes).
    // Used by AnnotateNewInstances to validate and retrieve resource guarantees.
    virtual const THashMap<std::string, NBundleControllerClient::TInstanceSizePtr>& GetInstanceSizes(
        const TZoneInfoPtr& zoneInfo) const = 0;

    // Iterates over all instances of this type, finds unannotated online ones,
    // and emits annotation mutations.
    virtual void AnnotateNewInstances(
        const TSchedulerInputState& input,
        const std::string& spareBundleName,
        const NBundleControllerClient::TInstanceResourcesPtr& resource,
        TSchedulerMutations* mutations) const = 0;

    // Returns a human-readable name for the "multiple sizes" alert id.
    virtual const std::string& GetAnnotateMultipleSizesAlertId() const = 0;

    // Returns a human-readable name for the "multiple zones" alert id.
    virtual const std::string& GetAnnotateMultipleZonesAlertId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAllocatorAdapter)

////////////////////////////////////////////////////////////////////////////////

IAllocatorAdapterPtr CreateRpcProxyAllocatorAdapter(
    TBundleControllerStatePtr state,
    const TDataCenterToInstanceMap& bundleProxies,
    const THashMap<std::string, THashSet<std::string>>& aliveProxies);

IAllocatorAdapterPtr CreateTabletNodeAllocatorAdapter(
    TBundleControllerStatePtr state,
    const TDataCenterToInstanceMap& bundleNodes,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
