#include "invoker_liveness_checker.h"

#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("InvokerChecker");

////////////////////////////////////////////////////////////////////////////////

TInvokerLivenessChecker::TInvokerLivenessChecker(
    IInvokerPtr invokerToCheck,
    TInvokerLivenessCheckerConfigPtr config,
    TString invokerName)
    : Config_(std::move(config))
    , InvokerToCheck_(std::move(invokerToCheck))
    , InvokerName_(std::move(invokerName))
    , CheckerQueue_(New<TActionQueue>("InvokerChecker"))
    , CheckerExecutor_(New<TPeriodicExecutor>(
        CheckerQueue_->GetInvoker(),
        BIND(&TInvokerLivenessChecker::DoCheck, MakeWeak(this)),
        Config_->Period))
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
            .WithTimeout(Config_->Timeout));

    if (!error.IsOK()) {
        YT_LOG_ERROR(
            "Invoker hung up (InvokerName: %v, Timeout: %v)",
            InvokerName_,
            Config_->Timeout);

        if (Config_->CoreDump) {
            YT_ABORT();
        } else {
            _exit(InvokerLivenessCheckerExitCode);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
