#include "job_registry.h"

#include <yt/yt/core/concurrency/bounded_concurrency_invoker.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

TJobRegistry::TJobRegistry(IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
{ }

void TJobRegistry::PutJob(TJobPtr job)
{
    // Assigned under the lock, destroyed after it: a retired job runs user
    // function and state store destructors, which must not execute (and may
    // not be fast) under a spin lock.
    TJobPtr retired;
    auto guard = Guard(Lock_);
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
        retired = std::move(entry.Job);
    }
    entry.Job = std::move(job);
    entry.RemovalPending = false;
}

void TJobRegistry::RemoveJob(const TJobId& jobId)
{
    // Declared before the guard so the removed job destructs after unlock.
    TJobPtr removed;
    auto guard = Guard(Lock_);
    auto it = Jobs_.find(jobId);
    if (it == Jobs_.end()) {
        return;
    }
    if (it->second.ActiveRequestCount > 0) {
        // In-flight batches (e.g. a timed-out RPC racing job teardown) must
        // keep their entry and serializing invoker; the last ReleaseJob
        // erases the entry.
        it->second.RemovalPending = true;
        return;
    }
    removed = std::move(it->second.Job);
    Jobs_.erase(it);
}

std::optional<TJobRegistry::TJobExecution> TJobRegistry::AcquireJob(const TJobId& jobId)
{
    auto guard = Guard(Lock_);
    auto it = Jobs_.find(jobId);
    if (it == Jobs_.end() || it->second.RemovalPending) {
        return std::nullopt;
    }
    ++it->second.ActiveRequestCount;
    return TJobExecution{
        .Job = it->second.Job,
        .Invoker = it->second.JobInvoker,
    };
}

void TJobRegistry::ReleaseJob(const TJobId& jobId)
{
    // Declared before the guard so the removed job destructs after unlock.
    TJobPtr removed;
    auto guard = Guard(Lock_);
    auto it = Jobs_.find(jobId);
    YT_VERIFY(it != Jobs_.end());
    YT_VERIFY(it->second.ActiveRequestCount > 0);
    --it->second.ActiveRequestCount;
    if (it->second.RemovalPending && it->second.ActiveRequestCount == 0) {
        removed = std::move(it->second.Job);
        Jobs_.erase(it);
    }
}

std::vector<TJobId> TJobRegistry::ListJobIds()
{
    auto guard = Guard(Lock_);
    return GetKeys(Jobs_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
