#pragma once

#include "public.h"

#include "semaphore_guard_options.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

// RAII interface for semaphore set.
// NB: TSemaphoreSet.Release() may throw, but because it is inside ~TSemaphoreSetGuard(),
// the exception is caught there. In order do check whether the lock was successful,
// you may want to call TSemaphoreSetGuard.Release() explicitly.
class TSemaphoreSetGuard
    : public TRefCounted
{
public:
    // NB: Throws if can not acquire semaphore in |options.AcquireOptions.AttemptCount| attempts.
    TSemaphoreSetGuard(
        IClientPtr client,
        TString semaphoreSetId,
        TSemaphoreGuardOptions options = TSemaphoreGuardOptions::GetDefault());

    ~TSemaphoreSetGuard();

    TString GetLeaseUuid() const;

    bool IsAcquired() const;

    // NB: Throws if can not release semaphore in |options.ReleaseOptions.AttemptCount| attempts.
    void Release();

private:
    IClientPtr Client_;
    const TString SemaphoreSetId_;
    const TString LeaseUuid_;
    const TSemaphoreGuardOptions Options_;

    std::atomic<bool> IsAcquired_{false};
    bool IsReleased_{false};

    NConcurrency::TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void Acquire();
};

DEFINE_REFCOUNTED_TYPE(TSemaphoreSetGuard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
