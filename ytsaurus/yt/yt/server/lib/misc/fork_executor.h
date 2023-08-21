#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/profiling/public.h>
#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/misc/error.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TForkCounters
    : public virtual TRefCounted
{
public:
    explicit TForkCounters(const NProfiling::TProfiler& profiler = {});

private:
    NProfiling::TEventTimer ForkDuration_;
    NProfiling::TEventTimer ChildDuration_;
    std::atomic<i64> ChildCount_ = 0;

    friend class TForkExecutor;
};

DEFINE_REFCOUNTED_TYPE(TForkCounters)

////////////////////////////////////////////////////////////////////////////////

//! Provides a generic infrastructure for executing fork.
class TForkExecutor
    : public virtual NLogging::TLoggerOwner
    , public virtual TRefCounted
{
public:
    TFuture<void> Fork();

protected:
    explicit TForkExecutor(TForkCountersPtr counters);

    ~TForkExecutor();

    //! Returns the timeout for running child process.
    virtual TDuration GetTimeout() const = 0;

    //! Returns the timeout for running fork. If process
    //! did not fork within this timeout, it crashes.
    virtual TDuration GetForkTimeout() const = 0;

    //! Called from the child process after fork.
    virtual void RunChild() = 0;

    //! Called from the parent process after fork.
    virtual void RunParent();

    //! Called from the parent process when child process is finished.
    virtual void Cleanup();

    //! Returns the invoker used for watching the child process.
    IInvokerPtr GetWatchdogInvoker();

private:
    pid_t ChildPid_ = -1;
    TPromise<void> Result_ = NewPromise<void>();
    TInstant StartTime_;
    NConcurrency::TActionQueuePtr WatchdogQueue_ = New<NConcurrency::TActionQueue>("ForkWD");
    NConcurrency::TPeriodicExecutorPtr WatchdogExecutor_;
    std::atomic<bool> Forked_ = false;
    TForkCountersPtr Counters_;

    void DoRunParent();
    void DoRunChild();

    void DoEndChild();

    void OnWatchdogCheck();

    void OnCanceled(const TError& error);
    void DoCancel(const TError& error);

    void DoCleanup();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
