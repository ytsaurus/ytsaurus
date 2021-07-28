#include "fair_share_queue_scheduler_thread.h"

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFairShareQueueSchedulerThread::TFairShareQueueSchedulerThread(
    TFairShareInvokerQueuePtr queue,
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadGroupName,
        threadName)
    , Queue_(std::move(queue))
{ }

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

