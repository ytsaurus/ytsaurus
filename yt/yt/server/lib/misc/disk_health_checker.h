#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

//! Checks disk health by writing a small file of random content
//! periodically, reading it, and comparing the content.
class TDiskHealthChecker
    : public TRefCounted
{
public:
    TDiskHealthChecker(
        TDiskHealthCheckerConfigPtr config,
        const TString& path,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        const NProfiling::TProfiler& profiler = {});

    //! Runs single health check.
    //! Don't call after #Start(), otherwise two checks may interfere.
    void RunCheck();

    void Start();

    TFuture<void> Stop();

    void OnDynamicConfigChanged(const TDiskHealthCheckerDynamicConfigPtr& newConfig);

    DEFINE_SIGNAL(void(const TError&), Failed);

private:
    const TDiskHealthCheckerConfigPtr Config_;
    const TString Path_;
    const IInvokerPtr CheckInvoker_;

    TAtomicIntrusivePtr<TDiskHealthCheckerDynamicConfig> DynamicConfig_;

    NLogging::TLogger Logger;

    NProfiling::TEventTimer TotalTimer_;
    NProfiling::TEventTimer ReadTimer_;
    NProfiling::TEventTimer WriteTimer_;

    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    TError RunCheckWithDeadline();
    void RunCheckWithTimeout();

    void OnCheck();
    void OnCheckCompleted(const TError& error);

    void DoRunCheck();

    TDuration GetWaitTimeout() const;
    TDuration GetExecTimeout() const;
    i64 GetTestSize() const;
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer

