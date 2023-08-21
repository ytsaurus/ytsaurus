#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger JobAgentServerLogger("JobAgent");

DECLARE_REFCOUNTED_STRUCT(IGpuInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
