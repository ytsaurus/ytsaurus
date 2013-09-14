#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/concurrency/periodic_executor.h>
#include <core/misc/error.h>

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

    void Start();

    DEFINE_SIGNAL(void(), Failed);

private:
    TDiskHealthCheckerConfigPtr Config;
    Stroka Path;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;
    TAtomic FailedLock;

    void OnCheck();
    void OnCheckCompleted(TError error);
    void OnCheckTimeout();

    TAsyncError RunCheck();

    void RaiseFailed();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

