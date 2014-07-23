#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/concurrency/periodic_executor.h>

#include <core/misc/error.h>

#include <core/logging/log.h>

#include <atomic>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Checks disk health by writing a small file of random content
 *  periodically, reading it, and comparing the content.
 */
class TDiskHealthChecker
    : public TRefCounted
{
public:
    TDiskHealthChecker(
        TDiskHealthCheckerConfigPtr config,
        const Stroka& path,
        IInvokerPtr invoker);

    //! Runs single health check. 
    //! Don't call after #Start(), otherwise two checks may interfere.
    TAsyncError RunCheck();

    void Start();

    DEFINE_SIGNAL(void(), Failed);

private:
    TDiskHealthCheckerConfigPtr Config_;
    Stroka Path_;

    IInvokerPtr CheckInvoker_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::atomic_flag FailedLock_;

    NLog::TLogger Logger;


    void OnCheck();
    void OnCheckCompleted(TError error);
    void OnCheckTimeout(TAsyncErrorPromise result);

    TError DoRunCheck();

};

DEFINE_REFCOUNTED_TYPE(TDiskHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

