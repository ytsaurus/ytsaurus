#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Check invoker liveness by executing simple actions.
//! Generate a core dump if timeout is exceeded.
class TInvokerLivenessChecker
    : public TRefCounted
{
public:
    TInvokerLivenessChecker(
        IInvokerPtr invokerToCheck,
        TInvokerLivenessCheckerConfigPtr config,
        TString invokerName);

    void Start();

    TFuture<void> Stop();

private:
    TInvokerLivenessCheckerConfigPtr Config_;
    IInvokerPtr InvokerToCheck_;
    TString InvokerName_;

    NConcurrency::TActionQueuePtr CheckerQueue_;
    NConcurrency::TPeriodicExecutorPtr CheckerExecutor_;

    void DoCheck();
};

DEFINE_REFCOUNTED_TYPE(TInvokerLivenessChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
