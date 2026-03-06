#pragma once

#include "public.h"

#include <yt/yt/client/bundle_controller_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TInstanceManager
{
public:
    void ManageInstances(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        ISpareInstanceAllocatorPtr spareInstances,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

private:
    static bool IsResourceUsageExceeded(
        const NBundleControllerClient::TInstanceResourcesPtr& usage,
        const TResourceQuotaPtr& quota);

    int GetAllocationCountInDataCenter(
        const TIndexedEntries<TAllocationRequestState>& allocationsState,
        const std::string& dataCenterName);

    void InitNewAllocations(
        const std::string& bundleName,
        const std::string& zoneName,
        const std::string& dataCenterName,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    int GetOutdatedInstanceCount(
        IAllocatorAdapter* adapter,
        const std::string& dataCenterName,
        const TSchedulerInputState& input,
        const std::string& bundleName,
        const TBundleInfoPtr& bundleInfo);

    std::string GetPodIdTemplate(
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    void ProcessExistingAllocations(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        ISpareInstanceAllocatorPtr spareInstances,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

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
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    void ProcessExistingDeallocations(
        const std::string& bundleName,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    void CreateHulkDeallocationRequest(
        const std::string& deallocationId,
        const std::string& instanceName,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations);

    void InitNewDeallocations(
        const std::string& bundleName,
        const std::string& zoneName,
        const std::string& dataCenterName,
        IAllocatorAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* /*mutations*/);

    std::string LocateAllocatedInstance(
        const TAllocationRequestPtr& requestInfo,
        const TSchedulerInputState& input) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
