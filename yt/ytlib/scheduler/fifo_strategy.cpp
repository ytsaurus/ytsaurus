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
    virtual void OnOperationStarted(TOperationPtr operation)
    {
        auto it = Queue.insert(Queue.end(), operation);
        YVERIFY(OpToIterator.insert(MakePair(operation, it)).second);
    }

    virtual void OnOperationFinished(TOperationPtr operation)
    {
        auto mapIt = OpToIterator.find(operation);
        YASSERT(mapIt != OpToIterator.end());
        Queue.erase(mapIt->second);
    }

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        // Process operations in FIFO order asking them to perform job scheduling.
        // Stop when no free slots are left.
        // Try not to schedule more then a single job from an operation to a node (no guarantees, though).
        int freeCount = node->Utilization().free_slot_count();
        FOREACH (auto operation, Queue) {
            if (freeCount == 0) {
                break;
            }

            if (operation->GetState() != EOperationState::Running) {
                continue;
            }
            
            int startCountBefore = static_cast<int>(jobsToStart->size());
            
            operation->GetController()->ScheduleJobs(
                node,
                jobsToStart,
                jobsToAbort);

            // Update utilization.
            int startCountAfter = static_cast<int>(jobsToStart->size());
            // Failure means that the controller has removed some of already scheduled jobs.
            YASSERT(startCountAfter >= startCountBefore);
            freeCount -= (startCountAfter - startCountBefore);
            // Failure means that the controller has scheduled more jobs than we had slots.
            YASSERT(freeCount >= 0);
            node->Utilization().set_free_slot_count(freeCount);
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

