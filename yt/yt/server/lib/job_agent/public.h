#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics;

DECLARE_REFCOUNTED_STRUCT(TResourceLimitsConfig)
DECLARE_REFCOUNTED_STRUCT(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_STRUCT(TMemoryPressureDetectorConfig)
DECLARE_REFCOUNTED_STRUCT(TJobResourceManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TJobResourceManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
