#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IGpuInfoProvider)

DECLARE_REFCOUNTED_STRUCT(TGpuInfoProviderConfigBase)
DECLARE_REFCOUNTED_STRUCT(TNvManagerGpuInfoProviderConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuAgentGpuInfoProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
