#pragma once

#include "private.h"
#include "scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr<TQueueImpl> queue,
        TIntrusivePtr<TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        int shutdownPriority = 0);

protected:
    const TInvokerQueuePtr<TQueueImpl> Queue_;
    typename TQueueImpl::TConsumerToken Token_;

    TEnqueuedAction CurrentAction_;

    TClosure BeginExecute() override;
    void EndExecute() override;

    void OnStart() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TSuspendableSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSuspendableSingleQueueSchedulerThread(
        TInvokerQueuePtr<TQueueImpl> queue,
        TIntrusivePtr<TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName);

    TFuture<void> Suspend(bool immediately);

    void Resume();

    void Shutdown(bool graceful);

protected:
    const TInvokerQueuePtr<TQueueImpl> Queue_;
    typename TQueueImpl::TConsumerToken Token_;

    TEnqueuedAction CurrentAction_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);

    std::atomic<bool> Suspending_ = false;

    std::atomic<bool> SuspendImmediately_ = false;
    TPromise<void> SuspendedPromise_ = NewPromise<void>();
    TIntrusivePtr<TEvent> ResumeEvent_;

    virtual TClosure BeginExecute() override;
    virtual void EndExecute() override;

    virtual void OnStart() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
