#include "fork_executor.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/proc.h>

#include <yt/core/ytree/serialize.h>

#ifdef _unix_
    // for wait*()
    #include <sys/wait.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto WatchdogCheckPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TForkExecutor::~TForkExecutor()
{
    YCHECK(ChildPid_ < 0);
}

TFuture<void> TForkExecutor::Fork()
{
#ifndef _unix_
    THROW_ERROR_EXCEPTION("Forks are not supported on this platform");
#else

    YCHECK(ChildPid_ < 0);

    try {
        YT_LOG_INFO("Going to fork");

        ChildPid_ = fork();
        if (ChildPid_ < 0) {
            THROW_ERROR_EXCEPTION("fork failed")
                << TError::FromSystem();
        }

        if (ChildPid_ == 0) {
            DoRunChild(); // never returns
            Y_UNREACHABLE();
        }

        DoRunParent();
        Result_.OnCanceled(BIND(&TForkExecutor::OnCanceled, MakeWeak(this)));
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error executing fork");
        DoCleanup();
        Result_.Set(ex);
    }

    return Result_;
#endif
}

void TForkExecutor::DoRunChild()
{
    try {
        RunChild();
        ::_exit(0);
    } catch (const std::exception& ex) {
        fprintf(stderr, "Child process failed:\n%s\n",
            ex.what());
        ::_exit(1);
    }
}

void TForkExecutor::DoRunParent()
{
    YT_LOG_INFO("Fork succeeded (ChildPid: %v)", ChildPid_);

    RunParent();

    StartTime_ = TInstant::Now();

    WatchdogExecutor_ = New<TPeriodicExecutor>(
        GetWatchdogInvoker(),
        BIND(&TForkExecutor::OnWatchdogCheck, MakeStrong(this)),
        WatchdogCheckPeriod);
    WatchdogExecutor_->Start();
}

void TForkExecutor::RunParent()
{ }

void TForkExecutor::Cleanup()
{ }

IInvokerPtr TForkExecutor::GetWatchdogInvoker()
{
    return WatchdogQueue_->GetInvoker();
}

void TForkExecutor::OnWatchdogCheck()
{
#ifdef _unix_
    if (ChildPid_ < 0)
        return;

    auto timeout = GetTimeout();
    if (TInstant::Now() > StartTime_ + timeout) {
        auto error = TError("Child process timed out")
            << TErrorAttribute("timeout", timeout);
        YT_LOG_ERROR(error);
        Result_.Set(error);
        DoCancel();
        return;
    }

    int status;
    if (::waitpid(ChildPid_, &status, WNOHANG) == 0)
        return;

    auto error = StatusToError(status);
    if (error.IsOK()) {
        YT_LOG_INFO("Child process finished (ChildPid: %v)", ChildPid_);
    } else {
        YT_LOG_ERROR(error, "Child process failed (ChildPid: %v)", ChildPid_);
    }
    Result_.Set(error);

    DoCleanup();
#endif
}

void TForkExecutor::DoCleanup()
{
    ChildPid_ = -1;
    if (WatchdogExecutor_) {
        WatchdogExecutor_->Stop();
        WatchdogExecutor_.Reset();
    }

    Cleanup();
}

void TForkExecutor::OnCanceled()
{
    YT_LOG_INFO("Fork executor canceled");
    GetWatchdogInvoker()->Invoke(
        BIND(&TForkExecutor::DoCancel, MakeStrong(this)));
}

void TForkExecutor::DoCancel()
{
    if (ChildPid_ < 0)
        return;

    YT_LOG_INFO("Killing child process (ChildPid: %v)", ChildPid_);

#ifdef _unix_
    ::kill(ChildPid_, SIGKILL);
    ::waitpid(ChildPid_, nullptr, 0);
#endif

    YT_LOG_INFO("Child process killed");

    Result_.TrySet(TError("Fork executor canceled"));
    DoCleanup();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
