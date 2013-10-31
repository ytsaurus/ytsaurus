#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NChunkHolder {

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
    TDiskHealthCheckerConfigPtr Config;
    Stroka Path;

    IInvokerPtr CheckInvoker;

    TPeriodicInvokerPtr PeriodicInvoker;
    TAtomic FailedLock;

    TCallback<TError (void)> CheckCallback;

    void OnCheck();
    void OnCheckCompleted(TError error);
    void OnCheckTimeout(TAsyncErrorPromise result);

    TError DoRunCheck();

};

////////////////////////////////////////////////////////////////////////////////

TAsyncError RunDiskHealthCheck(
    TDiskHealthCheckerConfigPtr config,
    const Stroka& path,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

