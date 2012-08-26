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
    virtual void OnOperationStarted(TOperationPtr operation) override
    {
        UNUSED(operation);
    }

    virtual void OnOperationFinished(TOperationPtr operation) override
    {
        UNUSED(operation);
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        // Refuse to do anything.
        UNUSED(context);
    }
};

TAutoPtr<ISchedulerStrategy> CreateNullStrategy()
{
    return new TNullStrategy();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

