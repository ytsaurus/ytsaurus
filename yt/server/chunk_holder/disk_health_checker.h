#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/misc/periodic_invoker.h>

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

    void Start();

    DEFINE_SIGNAL(void(), Failed);

private:
    TDiskHealthCheckerConfigPtr Config;
    Stroka Path;

    TPeriodicInvokerPtr PeriodicInvoker;
    TAtomic FailedLock;

    void OnCheck();
    void OnCheckSuccess();
    void OnCheckTimeout();

    TFuture<void> RunCheck();

    void RaiseFailed();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

