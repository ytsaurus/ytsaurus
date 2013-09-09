#include "stdafx.h"
#include "snapshot_builder_detail.h"

#include <core/concurrency/periodic_invoker.h>

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

static TDuration WatchdogCheckPeriod = TDuration::MilliSeconds(100);

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
    LOG_INFO("Fork success");

    Deadline = TInstant::Now() + GetTimeout();

    WatchdogInvoker = New<TPeriodicInvoker>(
        GetSyncInvoker(),
        BIND(&TSnapshotBuilderBase::OnWatchdogCheck, MakeStrong(this)),
        WatchdogCheckPeriod);
    WatchdogInvoker->Start();
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

    int result;
    if (waitpid(ChildPid, &result, WNOHANG) == 0)
        return;

    if (WIFEXITED(result)) {
        LOG_INFO("Snapshot child process finished");
        Result.Set(TError());
    } else {
        TError error("Snapshot child process exited with code %d",
            WEXITSTATUS(result));
        LOG_ERROR(error);
        Result.Set(error);
    }

    Cleanup();
#endif
}

void TSnapshotBuilderBase::Cleanup()
{
    ChildPid = -1;
    if (WatchdogInvoker) {
        WatchdogInvoker->Stop();
        WatchdogInvoker.Reset();
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
