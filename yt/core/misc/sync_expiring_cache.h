#pragma once

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TSyncExpiringCache
    : public TRefCounted
{
public:
    TSyncExpiringCache(
        TCallback<TValue(const TKey&)> calculateValueAction,
        TDuration expirationTimeout,
        IInvokerPtr invoker);

    TValue Get(const TKey& key);
    void Clear();

private:
    struct TEntry
    {
        NProfiling::TCpuInstant LastAccessTime;
        NProfiling::TCpuInstant LastUpdateTime;
        TValue Value;
    };

    NConcurrency::TReaderWriterSpinLock MapLock_;
    THashMap<TKey, TEntry> Map_;

    const TCallback<TValue(const TKey&)> CalculateValueAction_;
    const TDuration ExpirationTimeout_;
    NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    void DeleteExpiredItems();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_EXPIRING_CACHE_INL_H_
#include "sync_expiring_cache-inl.h"
#undef SYNC_EXPIRING_CACHE_INL_H_
