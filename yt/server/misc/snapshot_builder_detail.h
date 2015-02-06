#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Provides a generic infrastructure for building snapshots via fork.
class TSnapshotBuilderBase
    : public TRefCounted
{
public:
    TFuture<void> Run();

protected:
    ~TSnapshotBuilderBase();

    //! Must be initialized in the deriving class.
    NLog::TLogger Logger;

    //! Returns the timeout for building a snapshot.
    virtual TDuration GetTimeout() const = 0;

    //! Called from the child process after fork.
    virtual void RunChild() = 0;

    //! Called from the parent process after fork.
    virtual void RunParent();

    //! Called from the parent process when child process is finished.
    virtual void Cleanup();

    //! Returns the invoker used for watching the child process.
    IInvokerPtr GetWatchdogInvoker();

private:
    pid_t ChildPid_ = -1;
    TPromise<void> Result_ = NewPromise<void>();
    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr WatchdogExecutor_;


    void DoRunParent();
    void DoRunChild();

    void OnWatchdogCheck();

    void OnCanceled();
    void DoCancel();

    void DoCleanup();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
