#pragma once

#include "public.h"

#include "job.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Registry of jobs keyed by job id, owned by the worker: entries are created
//! and updated by PutJob and removed by RemoveJob, so an entry lives exactly
//! as long as its job.
class TJobRegistry
    : public TRefCounted
{
public:
    struct TJobExecution
    {
        TJobPtr Job;
        IInvokerPtr Invoker;
    };

    explicit TJobRegistry(IInvokerPtr invoker);

    //! Registers or replaces a job.
    void PutJob(TJobPtr job);
    //! Removes a job; unknown ids are ignored (removal is idempotent).
    //! An entry with active leases is only marked: it stops being acquirable
    //! immediately and is erased when the last lease is released, so the
    //! serializing invoker outlives every in-flight batch.
    void RemoveJob(const TJobId& jobId);
    //! Acquires an execution lease for a job. The lease keeps the registry
    //! entry and its serializing invoker alive until ReleaseJob() is called.
    //! Returns null when the job is unknown.
    //!
    //! The returned invoker admits one batch at a time (holding its slot
    //! across fiber suspensions in user code) and survives job replacement,
    //! so an RPC retry carrying the job info cannot race the original request.
    std::optional<TJobExecution> AcquireJob(const TJobId& jobId);
    //! Releases an execution lease, erasing the entry if its removal was
    //! deferred while the lease was held.
    void ReleaseJob(const TJobId& jobId);
    //! Ids of every registered job, including those whose removal is deferred
    //! behind an active lease.
    std::vector<TJobId> ListJobIds();

private:
    struct TEntry
    {
        TJobPtr Job;
        IInvokerPtr JobInvoker;
        int ActiveRequestCount = 0;
        bool RemovalPending = false;
    };

    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TJobId, TEntry> Jobs_;
};

DEFINE_REFCOUNTED_TYPE(TJobRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
