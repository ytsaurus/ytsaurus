#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void CleanPortoSlotResources(
    TNonNullPtr<TSlotLocation> location,
    int slotIndex,
    TNonNullPtr<IPortoVolumeManager> volumeManager,
    const TSlotManagerDynamicConfig& config,
    const THashSet<std::string>& preservedVolumePaths = {},
    bool initializing = false);

#ifdef _linux_

TFuture<void> CreatePortoRootVolumeDirectories(
    TNonNullPtr<TSlotLocation> location,
    const IVolumePtr& rootVolume,
    int userId);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
