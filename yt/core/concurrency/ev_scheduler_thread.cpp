#include "stdafx.h"
#include "ev_scheduler_thread.h"

namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TInvoker::TInvoker(TEVSchedulerThread* owner)
    : Owner(owner)
{ }

void TEVSchedulerThread::TInvoker::Invoke(const TClosure& callback)
{
    YASSERT(callback);

    if (!Owner->IsRunning())
        return;

    Owner->Queue.Enqueue(callback);
    Owner->CallbackWatcher.send();
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
TThreadId TEVSchedulerThread::TInvoker::GetThreadId() const
{
    return Owner->ThreadId;
}

bool TEVSchedulerThread::TInvoker::CheckAffinity(IInvokerPtr invoker) const
{
    return invoker.Get() == this;
}
#endif

///////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TEVSchedulerThread(
    const Stroka& threadName,
    bool enableLogging)
    : TSchedulerThread(
        &CallbackEventCount,
        threadName,
        NProfiling::EmptyTagIds,
        enableLogging,
        false)
    , CallbackWatcher(EventLoop)
    , Invoker(New<TInvoker>(this))
{
    CallbackWatcher.set<TEVSchedulerThread, &TEVSchedulerThread::OnCallback>(this);
    CallbackWatcher.start();
}

IInvokerPtr TEVSchedulerThread::GetInvoker()
{
    return Invoker;
}

void TEVSchedulerThread::OnShutdown()
{
    CallbackWatcher.send();
}

EBeginExecuteResult TEVSchedulerThread::BeginExecute()
{
    {
        auto result = BeginExecuteCallbacks();
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }
    }

    EventLoop.run(0);

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
    if (!IsRunning()) {
        return EBeginExecuteResult::Terminated;
    }

    TClosure callback;
    if (!Queue.Dequeue(&callback)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    try {
        TCurrentInvokerGuard guard(Invoker);
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
    EventLoop.break_loop();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
