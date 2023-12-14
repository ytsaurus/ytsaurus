#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics;

DECLARE_REFCOUNTED_CLASS(TGpuManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_CLASS(TMemoryPressureDetectorConfig)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TJobControllerConfig)
DECLARE_REFCOUNTED_CLASS(TShellCommandConfig)

DECLARE_REFCOUNTED_CLASS(TJobControllerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TGpuManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
