#include "ev_scheduler_thread.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TInvoker::TInvoker(TEVSchedulerThread* owner)
    : Owner_(owner)
{ }

void TEVSchedulerThread::TInvoker::Invoke(TClosure callback)
{
    Y_ASSERT(callback);
    Owner_->EnqueueCallback(std::move(callback));
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
TThreadId TEVSchedulerThread::TInvoker::GetThreadId() const
{
    return Owner_->ThreadId_;
}

bool TEVSchedulerThread::TInvoker::CheckAffinity(const IInvokerPtr& invoker) const
{
    return invoker.Get() == this;
}
#endif

////////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TEVSchedulerThread(
    const TString& threadName,
    bool enableLogging)
    : TSchedulerThread(
        std::make_shared<TEventCount>(),
        threadName,
        NProfiling::EmptyTagIds,
        enableLogging,
        false)
    , CallbackWatcher_(EventLoop_)
    , Invoker_(New<TInvoker>(this))
{
    CallbackWatcher_.set<TEVSchedulerThread, &TEVSchedulerThread::OnCallback>(this);
    CallbackWatcher_.start();
}

const IInvokerPtr& TEVSchedulerThread::GetInvoker()
{
    return Invoker_;
}

void TEVSchedulerThread::BeforeShutdown()
{
    CallbackWatcher_.send();
}

void TEVSchedulerThread::AfterShutdown()
{
    // Drain queue.
    TClosure callback;
    while (Queue_.Dequeue(&callback)) {
        callback.Reset();
    }

    YCHECK(Queue_.IsEmpty()); // As a side effect, this releases free lists.
}

EBeginExecuteResult TEVSchedulerThread::BeginExecute()
{
    {
        auto result = BeginExecuteCallbacks();
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }
    }

    EventLoop_.run(0);

    {
        auto result = BeginExecuteCallbacks();
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }
    }

    // NB: Never return QueueEmpty to prevent waiting on CallbackEventCount.
    return EBeginExecuteResult::Success;
}

EBeginExecuteResult TEVSchedulerThread::BeginExecuteCallbacks()
{
    TClosure callback;
    if (!Queue_.Dequeue(&callback)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    CallbackEventCount_->CancelWait();

    if (IsShutdown()) {
        return EBeginExecuteResult::Terminated;
    }

    try {
        TCurrentInvokerGuard guard(Invoker_);
        callback.Run();
        return EBeginExecuteResult::Success;
    } catch (const TFiberCanceledException&) {
        return EBeginExecuteResult::Terminated;
    }
}

void TEVSchedulerThread::EndExecute()
{ }

void TEVSchedulerThread::OnCallback(ev::async&, int)
{
    EventLoop_.break_loop();
}

void TEVSchedulerThread::EnqueueCallback(TClosure callback)
{
    if (IsShutdown()) {
        return;
    }

    Queue_.Enqueue(std::move(callback));
    CallbackWatcher_.send();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
