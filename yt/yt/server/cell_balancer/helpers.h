#pragma once

#include "public.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

int GetUsedSlotCount(const TTabletNodeInfoPtr& nodeInfo);

int GetTargetCellCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo);

int GetTargetDataCenterInstanceCount(int targetCount, const TZoneInfoPtr& zone);

TBundleControllerInstanceAnnotationsPtr GetBundleControllerInstanceAnnotationsToSet(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const TAllocationRequestPtr& allocationInfo,
    const TBundleControllerInstanceAnnotationsPtr& bundleControllerAnnotations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
