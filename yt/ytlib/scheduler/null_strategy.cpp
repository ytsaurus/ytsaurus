#include "stdafx.h"
#include "null_strategy.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TNullStrategy
    : public ISchedulerStrategy
{
public:
    virtual void OnOperationStarted(TOperationPtr operation)
    {
        UNUSED(operation);
    }

    virtual void OnOperationFinished(TOperationPtr operation)
    {
        UNUSED(operation);
    }

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        // Refuse to do anything.
        UNUSED(node);
        UNUSED(jobsToStart);
        UNUSED(jobsToAbort);
    }
};

TAutoPtr<ISchedulerStrategy> CreateNullStrategy()
{
    return new TNullStrategy();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

