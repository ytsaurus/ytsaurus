#include "helpers.h"

#include <yt/yt/server/scheduler/common/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

bool IsAssignmentPreliminary(const TAssignmentPtr& /*assignment*/)
{
    // TODO(eshcherbin): Should be true only for assignments without a running allocation.
    return true;
}

NLogging::TOneShotFluentLogEvent LogStructuredGpuEventFluently(EGpuSchedulingLogEventType eventType)
{
    return LogStructuredEventFluently(SchedulerGpuEventLogger(), NLogging::ELogLevel::Info)
        .Item("timestamp").Value(TInstant::Now())
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
