#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT {

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
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    //! Runs single health check. 
    //! Don't call after #Start(), otherwise two checks may interfere.
    TFuture<void> RunCheck();

    void Start();

    DEFINE_SIGNAL(void(const TError&), Failed);

private:
    const TDiskHealthCheckerConfigPtr Config_;
    const TString Path_;
    const IInvokerPtr CheckInvoker_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;


    void OnCheck();
    void OnCheckCompleted(const TError& error);

    void DoRunCheck();

};

DEFINE_REFCOUNTED_TYPE(TDiskHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

