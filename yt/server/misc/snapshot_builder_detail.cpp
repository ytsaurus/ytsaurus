#include "stdafx.h"
#include "snapshot_builder_detail.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/lazy_ptr.h>
#include <core/misc/proc.h>

#include <core/ytree/serialize.h>

#include <core/actions/invoker_util.h>

#if defined(_unix_)
    // for wait*()
    #include <sys/wait.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto WatchdogCheckPeriod = TDuration::MilliSeconds(100);
static TLazyIntrusivePtr<TActionQueue> WatchdogQueue(TActionQueue::CreateFactory("SnapshotWD"));

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilderBase::TSnapshotBuilderBase()
{
    ChildPid_ = -1;
}

TSnapshotBuilderBase::~TSnapshotBuilderBase()
{
    YCHECK(ChildPid_ < 0);
}

TAsyncError TSnapshotBuilderBase::Run()
{
    YCHECK(ChildPid_ < 0);

    try {
#ifdef _unix_
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
#else
        RunParent();
        RunChild();
        Result_.Set(TError());
#endif
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error building snapshot");
        Result_.Set(ex);
    }

    return Result_;
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
    LOG_INFO("Fork succeded (ChildPid: %v)", ChildPid_.load());

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
#if defined(_unix_)
    auto timeout = GetTimeout();
    if (TInstant::Now() > StartTime_ + timeout) {
        auto error = TError("Snapshot child process timed out")
            << TErrorAttribute("timeout", timeout);
        LOG_ERROR(error);
        Result_.Set(error);
        MaybeKillChild();
        return;
    }

    auto childPid = ChildPid_.load();
    if (childPid < 0)
        return;

    int status;
    if (waitpid(childPid, &status, WNOHANG) == 0)
        return;

    auto error = StatusToError(status);
    if (error.IsOK()) {
        LOG_INFO("Snapshot child process finished (ChildPid: %v)", childPid);
    } else {
        LOG_ERROR(error, "Snapshot child process failed (ChildPid: %v)", childPid);
    }
    Result_.Set(error);

    Cleanup();
#endif
}

void TSnapshotBuilderBase::OnCanceled()
{
    MaybeKillChild();
}

void TSnapshotBuilderBase::Cleanup()
{
    ChildPid_ = -1;
    if (WatchdogExecutor_) {
        WatchdogExecutor_->Stop();
        WatchdogExecutor_.Reset();
    }
}

void TSnapshotBuilderBase::MaybeKillChild()
{
    auto childPid = ChildPid_.load();
    if (childPid < 0)
        return;

    if (!ChildPid_.compare_exchange_strong(childPid, -1))
        return;

    Cleanup();

    LOG_INFO("Killing snapshot child process (ChildPid: %v)", childPid);
    WatchdogQueue->GetInvoker()->Invoke(BIND(&TSnapshotBuilderBase::DoKillChild, childPid));
}

void TSnapshotBuilderBase::DoKillChild(pid_t childPid)
{
#if defined(_unix_)
    kill(childPid, 9);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
