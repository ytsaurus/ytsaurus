#pragma once

#include "public.h"

#include "job.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! TTL cache of jobs keyed by job id (mirrors the Java/Python job caches).
//! An expired or missing job is healed by the worker resending the job info.
class TJobRegistry
    : public TRefCounted
{
public:
    struct TJobExecution
    {
        TJobPtr Job;
        IInvokerPtr Invoker;
    };

    TJobRegistry(TDuration jobTtl, IInvokerPtr invoker);

    void PutJob(TJobPtr job);
    //! Acquires an execution lease for a job. The lease protects the registry
    //! entry and its serializing invoker from TTL eviction until ReleaseJob()
    //! is called. Returns null when the job is unknown or expired.
    //!
    //! The returned invoker admits one batch at a time (holding its slot
    //! across fiber suspensions in user code) and survives job replacement,
    //! so an RPC retry carrying the job info cannot race the original request.
    std::optional<TJobExecution> AcquireJob(const TJobId& jobId);
    //! Releases an execution lease and refreshes the entry TTL.
    void ReleaseJob(const TJobId& jobId);

private:
    struct TEntry
    {
        TJobPtr Job;
        IInvokerPtr JobInvoker;
        int ActiveRequestCount = 0;
        TInstant LastAccess;
    };

    const TDuration JobTtl_;
    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TJobId, TEntry> Jobs_;

    bool IsExpired(const TEntry& entry, TInstant now) const;
    void SweepExpired(TInstant now, std::vector<TJobPtr>* retired);
};

DEFINE_REFCOUNTED_TYPE(TJobRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
