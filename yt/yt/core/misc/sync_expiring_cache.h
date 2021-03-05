#pragma once

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TSyncExpiringCache
    : public TRefCounted
{
public:
    TSyncExpiringCache(
        TCallback<TValue(const TKey&)> calculateValueAction,
        std::optional<TDuration> expirationTimeout,
        IInvokerPtr invoker);

    TValue Get(const TKey& key);
    std::optional<TValue> Find(const TKey& key);

    void Set(const TKey& key, TValue value);
    void Clear();

    void SetExpirationTimeout(std::optional<TDuration> expirationTimeout);

private:
    struct TEntry
    {
        NProfiling::TCpuInstant LastAccessTime;
        NProfiling::TCpuInstant LastUpdateTime;
        TValue Value;
    };

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, MapLock_);
    THashMap<TKey, TEntry> Map_;

    const TCallback<TValue(const TKey&)> CalculateValueAction_;
    std::atomic<NProfiling::TCpuDuration> ExpirationTimeout_;
    NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    void DeleteExpiredItems();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_EXPIRING_CACHE_INL_H_
#include "sync_expiring_cache-inl.h"
#undef SYNC_EXPIRING_CACHE_INL_H_
