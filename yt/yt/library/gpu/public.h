#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IGpuInfoProvider)

DECLARE_REFCOUNTED_STRUCT(TGpuInfoSourceConfigBase)
DECLARE_REFCOUNTED_STRUCT(TNvManagerGpuInfoProviderConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuAgentGpuInfoProviderConfig)

using TNetworkPriority = i8;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
