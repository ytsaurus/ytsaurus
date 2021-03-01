#pragma once

#include "scheduler_thread.h"
#include "spinlock.h"

#include <yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolBase
{
public:
    static constexpr int MaxThreadCount = 64;

    TThreadPoolBase(
        int threadCount,
        const TString& threadNamePrefix);

    void Configure(int threadCount);
    void Shutdown();

protected:
    const TString ThreadNamePrefix_;

    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    IInvokerPtr FinalizerInvoker_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    std::vector<TSchedulerThreadPtr> Threads_;


    void EnsureStarted();

    TString MakeThreadName(int index);

    virtual void DoStart();
    virtual void DoShutdown();
    virtual TClosure MakeFinalizerCallback();
    virtual void DoConfigure(int threadCount);

    virtual TSchedulerThreadPtr SpawnThread(int index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
