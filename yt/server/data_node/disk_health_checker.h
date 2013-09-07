#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/concurrency/periodic_invoker.h>
#include <ytlib/misc/error.h>

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

    TPeriodicInvokerPtr PeriodicInvoker;
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

