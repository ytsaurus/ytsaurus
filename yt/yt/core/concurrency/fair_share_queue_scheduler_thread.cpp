#include "fair_share_queue_scheduler_thread.h"

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFairShareQueueSchedulerThread::TFairShareQueueSchedulerThread(
    TFairShareInvokerQueuePtr queue,
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadName,
        NProfiling::TTagSet{}.WithTag(std::pair<TString, TString>("thread", threadName)))
    , Queue_(std::move(queue))
{ }

IInvokerPtr TFairShareQueueSchedulerThread::GetInvoker(int index)
{
    return Queue_->GetInvoker(index);
}

TClosure TFairShareQueueSchedulerThread::BeginExecute()
{
    return Queue_->BeginExecute(&CurrentAction_);
}

void TFairShareQueueSchedulerThread::EndExecute()
{
    Queue_->EndExecute(&CurrentAction_);
}

void TFairShareQueueSchedulerThread::OnStart()
{
    Queue_->SetThreadId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

