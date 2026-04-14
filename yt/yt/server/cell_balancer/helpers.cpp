#include "helpers.h"

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

int GetUsedSlotCount(const TTabletNodeInfoPtr& nodeInfo)
{
    int usedSlotCount = 0;

    for (const auto& slotInfo : nodeInfo->TabletSlots) {
        if (slotInfo->State != TabletSlotStateEmpty) {
            ++usedSlotCount;
        }
    }

    return usedSlotCount;
}

int GetTargetCellCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo)
{
    if (zoneInfo->DataCenters.empty()) {
        return 0;
    }

    const auto& targetConfig = bundleInfo->TargetConfig;
    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - zoneInfo->RedundantDataCenterCount;
    int activeNodeCount = (targetConfig->TabletNodeCount * activeDataCenterCount) / std::ssize(zoneInfo->DataCenters);

    YT_VERIFY(activeNodeCount >= 0);
    return activeNodeCount * targetConfig->CpuLimits->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize) / bundleInfo->Options->PeerCount;
}

int GetTargetDataCenterInstanceCount(
    int targetCount,
    const TZoneInfoPtr& zone)
{
    if (zone->DataCenters.empty()) {
        return 0;
    }

    return targetCount / std::ssize(zone->DataCenters);
}


TBundleControllerInstanceAnnotationsPtr GetBundleControllerInstanceAnnotationsToSet(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const TAllocationRequestPtr& allocationInfo,
    const TBundleControllerInstanceAnnotationsPtr& bundleControllerAnnotations)
{
    if (!bundleControllerAnnotations->AllocatedForBundle.empty() && bundleControllerAnnotations->Allocated) {
        return {};
    }
    auto result = NYTree::CloneYsonStruct(bundleControllerAnnotations);
    result->YPCluster = allocationInfo->Spec->YPCluster;
    result->NannyService = allocationInfo->Spec->NannyService;
    result->AllocatedForBundle = bundleName;
    result->DataCenter = dataCenterName;
    result->Allocated = true;
    result->DeallocationStrategy = DeallocationStrategyHulkRequest;
    ConvertToInstanceResources(*result->Resource, *allocationInfo->Spec->ResourceRequest);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
