#include "stdafx.h"
#include "fifo_strategy.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "exec_node.h"
#include "job.h"

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

