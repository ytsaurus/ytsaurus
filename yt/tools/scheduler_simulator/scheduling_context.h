#pragma once

#include "scheduler_strategy_host.h"

#include <yt/server/scheduler/scheduling_context_detail.h>
#include <yt/server/scheduler/exec_node.h>

#include <yt/server/controller_agent/operation_controller.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public NScheduler::TSchedulingContextBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, Now);

public:
    TSchedulingContext(
        NScheduler::TSchedulerConfigPtr schedulerConfig,
        NScheduler::TExecNodePtr node,
        const std::vector<NScheduler::TJobPtr>& runningJobs)
        : TSchedulingContextBase(schedulerConfig, node, runningJobs)
    { }

    void SetDurationForStartedJob(const TDuration& duration)
    {
        Durations_.push_back(duration);
    }

    const std::vector<TDuration>& GetStartedJobsDurations() const
    {
        return Durations_;
    }

private:
    std::vector<TDuration> Durations_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
