#pragma once

#include "public.h"

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TAuthCache
    : public virtual TRefCounted
{
public:
    TAuthCache(
        TAuthCacheConfigPtr config,
        NProfiling::TProfiler profiler = {});

    TFuture<TValue> Get(const TKey& key);

private:
    struct TEntry final
    {
        const TKey Key;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        TFuture<TValue> Future;
        TPromise<TValue> Promise;

        NConcurrency::TDelayedExecutorCookie EraseCookie;
        NProfiling::TCpuInstant LastAccessTime;

        NProfiling::TCpuInstant LastUpdateTime;
        bool Updating = false;

        bool IsOutdated(TDuration ttl, TDuration errorTtl);
        bool IsExpired(TDuration ttl);

        explicit TEntry(const TKey& key);
    };
    using TEntryPtr = TIntrusivePtr<TEntry>;

    const TAuthCacheConfigPtr Config_;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TKey, TEntryPtr> Cache_;

    virtual TFuture<TValue> DoGet(const TKey& key) noexcept = 0;
    void TryErase(const TWeakPtr<TEntry>& weakEntry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#define AUTH_CACHE_INL_H_
#include "auth_cache-inl.h"
#undef AUTH_CACHE_INL_H_
