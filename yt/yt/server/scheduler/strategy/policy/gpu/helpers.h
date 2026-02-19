#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

bool IsAssignmentPreliminary(const TAssignmentPtr& assignment);

NLogging::TOneShotFluentLogEvent LogStructuredGpuEventFluently(EGpuSchedulingLogEventType eventType);

NLogging::TLogger GetLogger(const std::string& treeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
