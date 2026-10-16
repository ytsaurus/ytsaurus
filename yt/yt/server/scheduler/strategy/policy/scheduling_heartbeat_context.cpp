#include "scheduling_heartbeat_context.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

void BuildScheduleAllocationsStatisticsCommon(const TScheduleAllocationsStatisticsPtr& statistics, NYTree::TFluentMap fluent)
{
    fluent
        .Item("preemptible_job_count").Value(statistics->PreemptibleAllocationCount)
        .Item("controller_schedule_job_count").Value(statistics->ControllerScheduleAllocationCount)
        .Item("controller_schedule_job_timed_out_count").Value(statistics->ControllerScheduleAllocationTimedOutCount)
        .Item("resource_usage").Value(statistics->ResourceUsage)
        .Item("resource_limits").Value(statistics->ResourceLimits);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
