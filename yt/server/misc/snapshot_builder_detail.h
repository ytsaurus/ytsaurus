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
    TSnapshotBuilderBase();
    ~TSnapshotBuilderBase();

    TAsyncError Run();

protected:
    //! Must be initialized in the deriving class.
    NLog::TLogger Logger;

    //! Returns the timeout for building a snapshot.
    virtual TDuration GetTimeout() const = 0;

    //! Called from the forked process to build the snapshot.
    virtual void Build() = 0;

private:
    std::atomic<pid_t> ChildPid_;
    TPromise<TError> Result_ = NewPromise<TError>();
    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr WatchdogExecutor_;


    void RunParent();
    void RunChild();

    void OnWatchdogCheck();
    void OnCanceled();

    void Cleanup();

    void MaybeKillChild();
    static void DoKillChild(pid_t childPid);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
