#include "stdafx.h"
#include "fifo_strategy.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TFifoStrategy
    : public ISchedulerStrategy
{
public:
    explicit TFifoStrategy(ISchedulerStrategyHost* host)
    {
        host->SubscribeOperationStarted(BIND(&TFifoStrategy::OnOperationStarted, this));
        host->SubscribeOperationFinished(BIND(&TFifoStrategy::OnOperationFinished, this));
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        // Process operations in FIFO order asking them to perform job scheduling.
        // Stop when no spare resources are left (coarse check).
        auto node = context->GetNode();
        FOREACH (auto operation, Queue) {
            while (context->HasSpareResources()) {
                if (operation->GetState() != EOperationState::Running) {
                    break;
                }
            
                auto job = operation->GetController()->ScheduleJob(context);
                if (!job) {
                    break;
                }
            }
        }
    }

    virtual void BuildOperationProgressYson(TOperationPtr operation, NYTree::IYsonConsumer* consumer) override
    {
        UNUSED(operation);
        UNUSED(consumer);
    }

    virtual void BuildOrchidYson(NYTree::IYsonConsumer* consumer) override
    {
        UNUSED(consumer);
    }

private:
    typedef std::list<TOperationPtr> TQueue;
    std::list<TOperationPtr> Queue;

    yhash_map<TOperationPtr, TQueue::iterator> OpToIterator;

    void OnOperationStarted(TOperationPtr operation)
    {
        auto it = Queue.insert(Queue.end(), operation);
        YCHECK(OpToIterator.insert(MakePair(operation, it)).second);
    }

    void OnOperationFinished(TOperationPtr operation)
    {
        auto mapIt = OpToIterator.find(operation);
        YASSERT(mapIt != OpToIterator.end());
        Queue.erase(mapIt->second);
    }

};

TAutoPtr<ISchedulerStrategy> CreateFifoStrategy(ISchedulerStrategyHost* host)
{
    YCHECK(host);
    return new TFifoStrategy(host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

