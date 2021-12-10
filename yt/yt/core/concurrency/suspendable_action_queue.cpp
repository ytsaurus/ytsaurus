#include "suspendable_action_queue.h"

#include "profiling_helpers.h"
#include "single_queue_scheduler_thread.h"

#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSuspendableActionQueue
    : public ISuspendableActionQueue
{
public:
    TSuspendableActionQueue(const TString& threadName)
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(threadName)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSuspendableSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName,
            threadName))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("SuspendableActionQueue(%v)", threadName),
            BIND(&TSuspendableActionQueue::Shutdown, MakeWeak(this), /*graceful*/ false),
            /*priority*/ 100))
    { }

    ~TSuspendableActionQueue()
    {
        Shutdown(/*graceful*/ false);
    }

    void Shutdown(bool graceful) final
    {
        if (Stopped_.exchange(true)) {
            return;
        }

        Queue_->Shutdown();

        FinalizerInvoker_->Invoke(BIND([graceful, thread = Thread_, queue = Queue_] {
            thread->Shutdown(graceful);
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    const IInvokerPtr& GetInvoker() override
    {
        EnsureStarted();
        return Invoker_;
    }

    TFuture<void> Suspend(bool immediately) override
    {
        auto future = Thread_->Suspend(immediately);

        // Invoke empty callback to wake up thread.
        Queue_->Invoke(BIND([] { }));

        return future;
    }

    void Resume() override
    {
        Thread_->Resume();
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSuspendableSingleQueueSchedulerThreadPtr Thread_;
    const TShutdownCookie ShutdownCookie_;

    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopped_ = false;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

    void EnsureStarted()
    {
        if (Started_.load(std::memory_order_relaxed)) {
            return;
        }
        if (Started_.exchange(true)) {
            return;
        }
        Thread_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

ISuspendableActionQueuePtr CreateSuspendableActionQueue(const TString& threadName)
{
    return New<TSuspendableActionQueue>(threadName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
