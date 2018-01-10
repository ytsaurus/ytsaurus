#pragma once

#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TExpiringCache
    : public TRefCounted
{
public:
    TExpiringCache(
        TCallback<TValue(TKey)> calculateValueAction,
        TDuration expirationTimeout,
        IInvokerPtr invoker);

    TValue Get(const TKey& key);

    void ForceUpdate();

    void Start();
    void Stop();

private:
    struct TEntry
    {
        NProfiling::TCpuInstant LastAccessTime;
        NProfiling::TCpuInstant LastUpdateTime;
        TValue Value;
    };


    NConcurrency::TReaderWriterSpinLock StoreLock_;
    THashMap<TKey, TEntry> Store_;

    const TCallback<TValue(TKey)> CalculateValueAction_;
    const TDuration ExpirationTimeout_;
    NConcurrency::TPeriodicExecutorPtr PeriodicDeleter_;

    void DeleteExpiredItems();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define CACHE_INL_H_
#include "cache-inl.h"
#undef CACHE_INL_H_
