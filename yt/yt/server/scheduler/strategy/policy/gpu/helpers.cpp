#include "helpers.h"
#include "structs.h"

#include <yt/yt/server/scheduler/common/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

bool IsAssignmentPreliminary(const TAssignmentPtr& assignment)
{
    return !assignment->AllocationId.has_value();
}

NLogging::TOneShotFluentLogEvent LogStructuredGpuEventFluently(EGpuSchedulingLogEventType eventType)
{
    return LogStructuredEventFluently(SchedulerGpuEventLogger(), NLogging::ELogLevel::Info)
        .Item("timestamp").Value(TInstant::Now())
        .Item("event_type").Value(eventType);
}

NLogging::TLogger GetLogger(const std::string& treeId)
{
    return GpuSchedulingPolicyLogger().WithTag("TreeId: %v", treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
