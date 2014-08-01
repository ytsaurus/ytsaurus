#include "stdafx.h"
#include "snapshot_builder_detail.h"

#include <core/concurrency/periodic_executor.h>
#include <core/misc/proc.h>

#include <core/actions/invoker_util.h>

#if defined(_unix_)
    // fork()
    #include <sys/types.h>
    #include <unistd.h>

    // for wait*()
    #include <sys/wait.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto WatchdogCheckPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilderBase::TSnapshotBuilderBase()
    : ChildPid(-1)
    , Result(NewPromise<TError>())
{ }

TSnapshotBuilderBase::~TSnapshotBuilderBase()
{
    // Last chance to kill the child.
    // No logging, no checks.
    KillChild();
}

TAsyncError TSnapshotBuilderBase::Run()
{
#if defined(_unix_)
    LOG_INFO("Going to fork");

    ChildPid = fork();
    if (ChildPid < 0) {
        return MakePromise(TError("Error building snapshot: fork failed")
            << TError::FromSystem());
    }

    if (ChildPid  == 0) {
        CloseAllDescriptors();
        RunChild();
        _exit(0);
    } else {
        RunParent();
    }
#else
    try {
        RunChild();
        Result.Set(TError());
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error building snapshot");
        Result.Set(ex);
    }
#endif
    return Result;
}

void TSnapshotBuilderBase::RunChild()
{
    Build();
}

void TSnapshotBuilderBase::RunParent()
{
    LOG_INFO("Fork success (ChildPid: %v)", ChildPid);

    Deadline = TInstant::Now() + GetTimeout();

    WatchdogExecutor = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TSnapshotBuilderBase::OnWatchdogCheck, MakeStrong(this)),
        WatchdogCheckPeriod);
    WatchdogExecutor->Start();
}

void TSnapshotBuilderBase::OnWatchdogCheck()
{
#if defined(_unix_)
    if (TInstant::Now() > Deadline) {
        TError error("Snapshot child process timed out");
        LOG_ERROR(error);
        Result.Set(error);
        KillChild();
        return;
    }

    int status;
    if (waitpid(ChildPid, &status, WNOHANG) == 0)
        return;

    auto error = StatusToError(status);
    if (error.IsOK()) {
        LOG_INFO("Snapshot child process finished (ChildPid: %v)", ChildPid);
    } else {
        LOG_ERROR(error, "Snapshot child process failed (ChildPid: %v)", ChildPid);
    }
    Result.Set(error);

    Cleanup();
#endif
}

void TSnapshotBuilderBase::Cleanup()
{
    ChildPid = -1;
    if (WatchdogExecutor) {
        WatchdogExecutor->Stop();
        WatchdogExecutor.Reset();
    }
}

void TSnapshotBuilderBase::KillChild()
{
#if defined(_unix_)
    if (ChildPid > 0) {
        kill(ChildPid, 9);
    }
#endif
    Cleanup();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
