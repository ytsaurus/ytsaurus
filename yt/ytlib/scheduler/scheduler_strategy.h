#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategy
{
    virtual ~ISchedulerStrategy()
    { }

    virtual void OnOperationStarted(TOperationPtr operation) = 0;
    virtual void OnOperationFinished(TOperationPtr operation) = 0;

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
