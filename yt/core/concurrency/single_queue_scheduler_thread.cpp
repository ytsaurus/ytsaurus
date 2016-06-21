#include "single_queue_scheduler_thread.h"

namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

TSingleQueueSchedulerThread::TSingleQueueSchedulerThread(
    TInvokerQueuePtr queue,
    std::shared_ptr<TEventCount> callbackEventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling,
    bool detached)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadName,
        tagIds,
        enableLogging,
        enableProfiling,
        detached)
    , Queue(std::move(queue))
{ }

TSingleQueueSchedulerThread::~TSingleQueueSchedulerThread()
{ }

IInvokerPtr TSingleQueueSchedulerThread::GetInvoker()
{
    return Queue;
}

EBeginExecuteResult TSingleQueueSchedulerThread::BeginExecute()
{
    return Queue->BeginExecute(&CurrentAction);
}

void TSingleQueueSchedulerThread::EndExecute()
{
    Queue->EndExecute(&CurrentAction);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
