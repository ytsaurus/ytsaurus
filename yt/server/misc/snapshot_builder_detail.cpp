#include "stdafx.h"
#include "snapshot_builder_detail.h"

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
static TLazyIntrusivePtr<TActionQueue> WatchdogQueue(TActionQueue::CreateFactory("SnapshotWD"));

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilderBase::~TSnapshotBuilderBase()
{
    YCHECK(ChildPid_ < 0);
}

TFuture<void> TSnapshotBuilderBase::Run()
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
        Result_.OnCanceled(BIND(&TSnapshotBuilderBase::OnCanceled, MakeWeak(this)));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error building snapshot");
        Result_.Set(ex);
    }

    return Result_;
#endif
}

void TSnapshotBuilderBase::DoRunChild()
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

void TSnapshotBuilderBase::DoRunParent()
{
    LOG_INFO("Fork succeded (ChildPid: %v)", ChildPid_);

    RunParent();
    
    StartTime_ = TInstant::Now();

    WatchdogExecutor_ = New<TPeriodicExecutor>(
        WatchdogQueue->GetInvoker(),
        BIND(&TSnapshotBuilderBase::OnWatchdogCheck, MakeStrong(this)),
        WatchdogCheckPeriod);
    WatchdogExecutor_->Start();
}

void TSnapshotBuilderBase::RunParent()
{ }

IInvokerPtr TSnapshotBuilderBase::GetWatchdogInvoker()
{
    return WatchdogQueue->GetInvoker();
}

void TSnapshotBuilderBase::OnWatchdogCheck()
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
    if (waitpid(ChildPid_, &status, WNOHANG) == 0)
        return;

    auto error = StatusToError(status);
    if (error.IsOK()) {
        LOG_INFO("Snapshot child process finished (ChildPid: %v)", ChildPid_);
    } else {
        LOG_ERROR(error, "Snapshot child process failed (ChildPid: %v)", ChildPid_);
    }
    Result_.Set(error);

    Cleanup();
#endif
}

void TSnapshotBuilderBase::Cleanup()
{
    ChildPid_ = -1;
    if (WatchdogExecutor_) {
        WatchdogExecutor_->Stop();
        WatchdogExecutor_.Reset();
    }
}

void TSnapshotBuilderBase::OnCanceled()
{
    LOG_INFO("Snapshot builder canceled");
    WatchdogQueue->GetInvoker()->Invoke(BIND(&TSnapshotBuilderBase::DoCancel, MakeStrong(this)));
}

void TSnapshotBuilderBase::DoCancel()
{
    if (ChildPid_ < 0)
        return;

    LOG_INFO("Killing snapshot child process (ChildPid: %v)", ChildPid_);

#ifdef _unix_
    ::kill(ChildPid_, 9);
#endif

    Result_.TrySet(TError("Snapshot builder canceled"));

    Cleanup();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
