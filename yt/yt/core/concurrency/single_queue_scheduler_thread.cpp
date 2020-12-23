#include "single_queue_scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TSingleQueueSchedulerThread<TQueueImpl>::TSingleQueueSchedulerThread(
    TInvokerQueuePtr<TQueueImpl> queue,
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName,
    const NProfiling::TTagSet& tagIds)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadName,
        tagIds)
    , Queue_(std::move(queue))
{ }

template <class TQueueImpl>
TClosure TSingleQueueSchedulerThread<TQueueImpl>::BeginExecute()
{
    return Queue_->BeginExecute(&CurrentAction_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::EndExecute()
{
    Queue_->EndExecute(&CurrentAction_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::OnStart()
{
    Queue_->SetThreadId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

template class TSingleQueueSchedulerThread<TMpmcQueueImpl>;
template class TSingleQueueSchedulerThread<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
