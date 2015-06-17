#include "stdafx.h"
#include "fork_snapshot_builder.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/lazy_ptr.h>
#include <core/misc/proc.h>

#include <core/ytree/serialize.h>

#ifdef _unix_
    // for wait*()
    #include <sys/wait.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto WatchdogCheckPeriod = TDuration::MilliSeconds(100);
static const TLazyIntrusivePtr<TActionQueue> WatchdogQueue(TActionQueue::CreateFactory("SnapshotWD"));

////////////////////////////////////////////////////////////////////////////////

TForkSnapshotBuilderBase::~TForkSnapshotBuilderBase()
{
    YCHECK(ChildPid_ < 0);
}

TFuture<void> TForkSnapshotBuilderBase::Fork()
{
#ifndef _unix_
    THROW_ERROR_EXCEPTION("Building snapshots is not supported on this platform");
#else

    YCHECK(ChildPid_ < 0);

    try {
        LOG_INFO("Going to fork");

        ChildPid_ = fork();
        if (ChildPid_ < 0) {
            THROW_ERROR_EXCEPTION("fork failed")
                << TError::FromSystem();
        }

        if (ChildPid_ == 0) {
            DoRunChild(); // never returns
            YUNREACHABLE();
        }

        DoRunParent();
        Result_.OnCanceled(BIND(&TForkSnapshotBuilderBase::OnCanceled, MakeWeak(this)));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error building snapshot");
        DoCleanup();
        Result_.Set(ex);
    }

    return Result_;
#endif
}

void TForkSnapshotBuilderBase::DoRunChild()
{
    try {
        RunChild();
        ::_exit(0);
    } catch (const std::exception& ex) {
        fprintf(stderr, "Snapshot builder child process failed:\n%s\n",
            ex.what());
        ::_exit(1);
    }
}

void TForkSnapshotBuilderBase::DoRunParent()
{
    LOG_INFO("Fork succeded (ChildPid: %v)", ChildPid_);

    RunParent();
    
    StartTime_ = TInstant::Now();

    WatchdogExecutor_ = New<TPeriodicExecutor>(
        WatchdogQueue->GetInvoker(),
        BIND(&TForkSnapshotBuilderBase::OnWatchdogCheck, MakeStrong(this)),
        WatchdogCheckPeriod);
    WatchdogExecutor_->Start();
}

void TForkSnapshotBuilderBase::RunParent()
{ }

void TForkSnapshotBuilderBase::Cleanup()
{ }

IInvokerPtr TForkSnapshotBuilderBase::GetWatchdogInvoker()
{
    return WatchdogQueue->GetInvoker();
}

void TForkSnapshotBuilderBase::OnWatchdogCheck()
{
#ifdef _unix_
    if (ChildPid_ < 0)
        return;

    auto timeout = GetTimeout();
    if (TInstant::Now() > StartTime_ + timeout) {
        auto error = TError("Snapshot child process timed out")
            << TErrorAttribute("timeout", timeout);
        LOG_ERROR(error);
        Result_.Set(error);
        DoCancel();
        return;
    }

    int status;
    if (::waitpid(ChildPid_, &status, WNOHANG) == 0)
        return;

    auto error = StatusToError(status);
    if (error.IsOK()) {
        LOG_INFO("Snapshot child process finished (ChildPid: %v)", ChildPid_);
    } else {
        LOG_ERROR(error, "Snapshot child process failed (ChildPid: %v)", ChildPid_);
    }
    Result_.Set(error);

    DoCleanup();
#endif
}

void TForkSnapshotBuilderBase::DoCleanup()
{
    ChildPid_ = -1;
    if (WatchdogExecutor_) {
        WatchdogExecutor_->Stop();
        WatchdogExecutor_.Reset();
    }

    Cleanup();
}

void TForkSnapshotBuilderBase::OnCanceled()
{
    LOG_INFO("Snapshot builder canceled");
    WatchdogQueue->GetInvoker()->Invoke(BIND(&TForkSnapshotBuilderBase::DoCancel, MakeStrong(this)));
}

void TForkSnapshotBuilderBase::DoCancel()
{
    if (ChildPid_ < 0)
        return;

    LOG_INFO("Killing snapshot child process (ChildPid: %v)", ChildPid_);

#ifdef _unix_
    ::kill(ChildPid_, 9);
    ::waitpid(ChildPid_, nullptr, 0);
#endif

    LOG_INFO("Snapshot child process killed");

    Result_.TrySet(TError("Snapshot builder canceled"));
    DoCleanup();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
