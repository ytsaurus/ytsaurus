#pragma once

#include "public.h"

#include <yt/yt/client/bundle_controller_client/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TInstanceManager
{
public:
    TInstanceManager(
        std::string bundleName,
        const TSchedulerInputState& input,
        ISpareInstanceAllocatorPtr spareInstanceAllocator,
        IAllocatorAdapter* adapter,
        TSchedulerMutations* mutations);

    // Processes existing allocations/deallocations and schedules new ones.
    void ManageInstances();

private:
    const std::string BundleName_;
    const TSchedulerInputState& Input_;
    const ISpareInstanceAllocatorPtr SpareInstanceAllocator_;
    IAllocatorAdapter* const Adapter_;
    TSchedulerMutations* const Mutations_;

    NLogging::TLogger Logger;

    static bool IsResourceUsageExceeded(
        const NBundleControllerClient::TInstanceResourcesPtr& usage,
        const TResourceQuotaPtr& quota);

    int GetAllocationCountInDataCenter(
        const TIndexedEntries<TAllocationRequestState>& allocationsState,
        const std::string& dataCenterName);

    bool CheckInstanceCountLimitReached(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo);

    bool CheckResourceUsageExceeded(const TResourceQuotaPtr& resourceQuota);

    // Creates new allocation request and makes |NewAllocations| mutation.
    TAllocationRequestStatePtr DoCreateAllocationRequest(
        const std::string& allocationId,
        const TBundleInfoPtr& bundleInfo,
        const std::string& dataCenterName,
        const TDataCenterInfoPtr& dataCenterInfo,
        const TZoneInfoPtr& zoneInfo);

    // Creates required amount of allocation requests. Modifies
    // |AllocationsState| and indirectly makes |NewAllocations| mutation.
    void InitNewAllocations(
        const std::string& zoneName,
        const std::string& dataCenterName);

    // Returns the number of instances that should be reallocated:
    //   * resources differ from bundle target resources
    //   * CMS maintenance is requested
    int GetInstanceCountToReallocate(const std::string& dataCenterName);

    std::string GetPodIdTemplate(
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    // Handles instance allocation pipeline.
    //   * Waits for the pod to be allocated.
    //   * Waits for the adapter to set all required instance attributes.
    //   * Waits for the instance to become online (see |GetAliveInstances|
    //     in Adapter_->EnsureAllocatedInstanceTagsSet)
    // Fires alerts if allocation is not found, failed, or stuck.
    // Returns false if current deallocation should not be tracked any more.
    bool ProcessAllocation(
        const std::string& allocationId,
        const TAllocationRequestStatePtr& allocationState);

    // Handles existing allocations and removes them from |AllocationsState|
    // on completion.
    void ProcessExistingAllocations();

    void CompleteExistingAllocationWithoutInstanceAllocatorService(
        const std::string& allocationId,
        const auto& allocationInfo,
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        ISpareInstanceAllocatorPtr spareInstances,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    bool ReturnToBundleBalancer(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    bool ReturnToSpareBundle(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    bool ProcessHulkDeallocation(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    // Returns false if current deallocation should not be tracked any more.
    bool ProcessDeallocation(
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState);

    void ProcessExistingDeallocations();

    void CreateHulkDeallocationRequest(
        const std::string& deallocationId,
        const std::string& instanceName,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    void InitNewDeallocations(
        const std::string& zoneName,
        const std::string& dataCenterName);

    // Finds the address of the instance which was allocated by allocation request,
    // if any.
    std::string LocateAllocatedInstance(
        const TAllocationRequestPtr& requestInfo,
        const TSchedulerInputState& input) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
