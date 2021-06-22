#include "invoker_liveness_checker.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("InvokerChecker");

////////////////////////////////////////////////////////////////////////////////

TInvokerLivenessChecker::TInvokerLivenessChecker(
    IInvokerPtr invokerToCheck,
    TDuration period,
    TDuration timeout,
    TString invokerName)
    : CheckerQueue_(New<TActionQueue>("InvokerChecker"))
    , CheckerExecutor_(New<TPeriodicExecutor>(
        CheckerQueue_->GetInvoker(),
        BIND(&TInvokerLivenessChecker::DoCheck, MakeWeak(this)),
        period))
    , InvokerToCheck_(std::move(invokerToCheck))
    , Timeout_(timeout)
    , InvokerName_(std::move(invokerName))
{ }

void TInvokerLivenessChecker::Start()
{
    CheckerExecutor_->Start();
}

TFuture<void> TInvokerLivenessChecker::Stop()
{
    return CheckerExecutor_->Stop();
}

void TInvokerLivenessChecker::DoCheck()
{
    auto simpleAction = BIND([]{});

    auto error = WaitFor(
        simpleAction
            .AsyncVia(InvokerToCheck_)
            .Run()
            .WithTimeout(Timeout_));

    // Core dump on timeout.
    YT_LOG_FATAL_IF(
        !error.IsOK(),
        "Invoker hanged up (InvokerName: %v, Timeout: %v)",
        InvokerName_,
        Timeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
