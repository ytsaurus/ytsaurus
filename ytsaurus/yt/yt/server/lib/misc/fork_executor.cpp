#include "fork_executor.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/serialize.h>

#ifdef _unix_
    // for wait*()
    #include <sys/wait.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto WatchdogCheckPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TForkCounters::TForkCounters(const NProfiling::TProfiler& profiler)
    : ForkDuration_(profiler.WithPrefix("/fork_executor").Timer("/fork_duration"))
    , ChildDuration_(profiler.WithPrefix("/fork_executor").Timer("/child_duration"))
{
    profiler
        .WithPrefix("/fork_executor")
        .AddFuncGauge("/child_count", MakeStrong(this), [this] {
            return ChildCount_.load();
        });
}

////////////////////////////////////////////////////////////////////////////////

TForkExecutor::TForkExecutor(TForkCountersPtr counters)
    : Counters_(std::move(counters))
{ }

TForkExecutor::~TForkExecutor()
{
    YT_VERIFY(ChildPid_ < 0);
}

TFuture<void> TForkExecutor::Fork()
{
#ifndef _unix_
    THROW_ERROR_EXCEPTION("Forks are not supported on this platform");
#else

    YT_VERIFY(ChildPid_ < 0);

    try {
        YT_LOG_INFO("Going to fork");

        TDelayedExecutor::Submit(
            BIND([this, this_ = MakeWeak(this)] {
                auto executor = this_.Lock();
                if (!executor) {
                    return;
                }

                YT_LOG_FATAL_UNLESS(Forked_, "Process did not fork within timeout; terminating (ForkTimeout: %v)",
                    GetForkTimeout());
            }),
            GetForkTimeout());

        auto forkStartTime = GetCpuInstant();

        ChildPid_ = fork();
        Forked_ = true;

        if (ChildPid_ != 0) {
            auto forkEndTime = GetCpuInstant();
            Counters_->ForkDuration_.Record(CpuDurationToDuration(forkEndTime - forkStartTime));
        }

        if (ChildPid_ < 0) {
            THROW_ERROR_EXCEPTION("fork failed")
                << TError::FromSystem();
        }

        if (ChildPid_ == 0) {
            DoRunChild(); // never returns
            YT_ABORT();
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
    Counters_->ChildCount_.fetch_add(1);

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
        DoCancel(error);
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

    DoEndChild();
#endif
}

void TForkExecutor::DoCleanup()
{
    ChildPid_ = -1;
    if (WatchdogExecutor_) {
        YT_UNUSED_FUTURE(WatchdogExecutor_->Stop());
        WatchdogExecutor_.Reset();
    }

    Cleanup();
}

void TForkExecutor::DoEndChild()
{
    YT_VERIFY(ChildPid_ > 0);

    Counters_->ChildDuration_.Record(TInstant::Now() - StartTime_);
    Counters_->ChildCount_.fetch_sub(1);

    DoCleanup();
}

void TForkExecutor::OnCanceled(const TError& error)
{
    YT_LOG_INFO(error, "Fork executor canceled");
    GetWatchdogInvoker()->Invoke(
        BIND(&TForkExecutor::DoCancel, MakeStrong(this), error));
}

void TForkExecutor::DoCancel(const TError& error)
{
    if (ChildPid_ < 0) {
        return;
    }

    YT_LOG_INFO("Killing child process (ChildPid: %v)", ChildPid_);

#ifdef _unix_
    ::kill(ChildPid_, SIGKILL);
    ::waitpid(ChildPid_, nullptr, 0);
#endif

    YT_LOG_INFO("Child process killed");

    Result_.TrySet(TError(NYT::EErrorCode::Canceled, "Fork executor canceled")
        << error);
    DoEndChild();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
