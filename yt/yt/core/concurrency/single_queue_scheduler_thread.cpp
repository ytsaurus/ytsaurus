#include "single_queue_scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TSingleQueueSchedulerThread<TQueueImpl>::TSingleQueueSchedulerThread(
    TInvokerQueuePtr<TQueueImpl> queue,
    TIntrusivePtr<TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName,
    int shutdownPriority)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadGroupName,
        threadName,
        shutdownPriority)
    , Queue_(std::move(queue))
    , Token_(Queue_->MakeConsumerToken())
{ }

template <class TQueueImpl>
TClosure TSingleQueueSchedulerThread<TQueueImpl>::BeginExecute()
{
    return Queue_->BeginExecute(&CurrentAction_, &Token_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::EndExecute()
{
    Queue_->EndExecute(&CurrentAction_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::OnStart()
{
    Queue_->SetThreadId(GetThreadId());
}

////////////////////////////////////////////////////////////////////////////////

template class TSingleQueueSchedulerThread<TMpmcQueueImpl>;
template class TSingleQueueSchedulerThread<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
