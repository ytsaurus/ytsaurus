#include "job_registry.h"

#include <yt/yt/core/concurrency/bounded_concurrency_invoker.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

TJobRegistry::TJobRegistry(TDuration jobTtl, IInvokerPtr invoker)
    : JobTtl_(jobTtl)
    , Invoker_(std::move(invoker))
{ }

void TJobRegistry::PutJob(TJobPtr job)
{
    auto now = TInstant::Now();
    // Filled under the lock, destroyed after it: retired jobs run user
    // function and state store destructors, which must not execute (and may
    // not be fast) under a spin lock.
    std::vector<TJobPtr> retired;
    {
        auto guard = Guard(Lock_);
        SweepExpired(now, &retired);
        auto& entry = Jobs_[job->GetJobId()];
        if (!entry.JobInvoker) {
            // NB: A serialized invoker would release exclusivity on the first
            // fiber suspension inside user code; the bounded invoker holds its
            // slot until the batch callback returns.
            entry.JobInvoker = NConcurrency::CreateBoundedConcurrencyInvoker(
                Invoker_,
                /*maxConcurrentInvocations*/ 1);
        }
        if (entry.Job) {
            retired.push_back(std::move(entry.Job));
        }
        entry.Job = std::move(job);
        entry.LastAccess = now;
    }
}

std::optional<TJobRegistry::TJobExecution> TJobRegistry::AcquireJob(const TJobId& jobId)
{
    auto now = TInstant::Now();
    // Declared before the guard so the expired job destructs after unlock.
    TJobPtr expired;
    auto guard = Guard(Lock_);
    auto it = Jobs_.find(jobId);
    if (it == Jobs_.end()) {
        return std::nullopt;
    }
    if (IsExpired(it->second, now)) {
        expired = std::move(it->second.Job);
        Jobs_.erase(it);
        return std::nullopt;
    }
    ++it->second.ActiveRequestCount;
    it->second.LastAccess = now;
    return TJobExecution{
        .Job = it->second.Job,
        .Invoker = it->second.JobInvoker,
    };
}

void TJobRegistry::ReleaseJob(const TJobId& jobId)
{
    auto guard = Guard(Lock_);
    auto it = Jobs_.find(jobId);
    YT_VERIFY(it != Jobs_.end());
    YT_VERIFY(it->second.ActiveRequestCount > 0);
    --it->second.ActiveRequestCount;
    it->second.LastAccess = TInstant::Now();
}

bool TJobRegistry::IsExpired(const TEntry& entry, TInstant now) const
{
    return entry.ActiveRequestCount == 0 && now - entry.LastAccess > JobTtl_;
}

void TJobRegistry::SweepExpired(TInstant now, std::vector<TJobPtr>* retired)
{
    for (auto it = Jobs_.begin(); it != Jobs_.end();) {
        if (IsExpired(it->second, now)) {
            retired->push_back(std::move(it->second.Job));
            Jobs_.erase(it++);
        } else {
            ++it;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
