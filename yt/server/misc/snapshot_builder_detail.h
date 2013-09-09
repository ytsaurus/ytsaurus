#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/concurrency/periodic_invoker.h>

#include <core/logging/log.h>

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
    pid_t ChildPid;
    TPromise<TError> Result;
    TInstant Deadline;
    NConcurrency::TPeriodicInvokerPtr WatchdogInvoker;

    void RunParent();
    void RunChild();

    void OnWatchdogCheck();

    void Cleanup();
    void KillChild();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
