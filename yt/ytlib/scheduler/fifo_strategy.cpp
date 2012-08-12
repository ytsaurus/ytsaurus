#include "stdafx.h"
#include "fifo_strategy.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"
#include "job_resources.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TFifoStrategy
    : public ISchedulerStrategy
{
public:
    virtual void OnOperationStarted(TOperationPtr operation)
    {
        auto it = Queue.insert(Queue.end(), operation);
        YCHECK(OpToIterator.insert(MakePair(operation, it)).second);
    }

    virtual void OnOperationFinished(TOperationPtr operation)
    {
        auto mapIt = OpToIterator.find(operation);
        YASSERT(mapIt != OpToIterator.end());
        Queue.erase(mapIt->second);
    }

    virtual void ScheduleJobs(ISchedulingContext* context)
    {
        // Process operations in FIFO order asking them to perform job scheduling.
        // Stop when no spare resources are left (coarse check).
        auto node = context->GetNode();
        FOREACH (auto operation, Queue) {
            while (HasSpareResources(node->ResourceUtilization(), node->ResourceLimits())) {
                if (operation->GetState() != EOperationState::Running) {
                    break;
                }
            
                auto job = operation->GetController()->ScheduleJob(context);
                if (!job) {
                    break;
                }

                IncreaseResourceUtilization(
                    &node->ResourceUtilization(),
                    job->GetSpec()->resource_utilization());
            }
        }
    }

private:
    typedef std::list<TOperationPtr> TQueue;
    std::list<TOperationPtr> Queue;

    yhash_map<TOperationPtr, TQueue::iterator> OpToIterator;

};

TAutoPtr<ISchedulerStrategy> CreateFifoStrategy()
{
    return new TFifoStrategy();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

