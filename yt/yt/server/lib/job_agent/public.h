#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics;

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_CLASS(TMemoryPressureDetectorConfig)
DECLARE_REFCOUNTED_CLASS(TJobResourceManagerConfig)
DECLARE_REFCOUNTED_CLASS(TJobResourceManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
