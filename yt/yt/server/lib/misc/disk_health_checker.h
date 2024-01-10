#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/sensor.h>

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
        const NProfiling::TProfiler& profiler = {});

    //! Runs single health check.
    //! Don't call after #Start(), otherwise two checks may interfere.
    TFuture<void> RunCheck();

    void Start();

    void Stop();

    DEFINE_SIGNAL(void(const TError&), Failed);

private:
    const TDiskHealthCheckerConfigPtr Config_;
    const TString Path_;
    const IInvokerPtr CheckInvoker_;

    NLogging::TLogger Logger;
    NProfiling::TEventTimer TotalTimer_;
    NProfiling::TEventTimer ReadTimer_;
    NProfiling::TEventTimer WriteTimer_;

    NConcurrency::TDelayedExecutorCookie CheckerCookie_;

    void OnCheck();
    void OnCheckCompleted(const TError& error);

    void DoRunCheck();

};

DEFINE_REFCOUNTED_TYPE(TDiskHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

