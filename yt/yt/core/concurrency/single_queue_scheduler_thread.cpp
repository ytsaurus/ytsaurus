#include "single_queue_scheduler_thread.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TSingleQueueSchedulerThread::TSingleQueueSchedulerThread(
    TInvokerQueuePtr queue,
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadName,
        tagIds,
        enableLogging,
        enableProfiling)
    , Queue(std::move(queue))
{ }

TClosure TSingleQueueSchedulerThread::BeginExecute()
{
    return Queue->BeginExecute(&CurrentAction);
}

void TSingleQueueSchedulerThread::EndExecute()
{
    Queue->EndExecute(&CurrentAction);
}

void TSingleQueueSchedulerThread::OnStart()
{
    Queue->SetThreadId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
