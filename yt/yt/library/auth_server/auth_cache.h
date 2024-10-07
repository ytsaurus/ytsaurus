#pragma once

#include "public.h"

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class TContext>
class TAuthCache
    : public virtual TRefCounted
{
public:
    explicit TAuthCache(
        TAuthCacheConfigPtr config,
        NProfiling::TProfiler profiler = {});

    TFuture<TValue> Get(const TKey& key, const TContext& context);

private:
    struct TEntry final
    {
        const TKey Key;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        TContext Context;
        TFuture<TValue> Future;
        TPromise<TValue> Promise;

        NConcurrency::TDelayedExecutorCookie EraseCookie;
        NProfiling::TCpuInstant LastAccessTime;

        NProfiling::TCpuInstant LastUpdateTime;
        bool Updating = false;

        bool IsOutdated(TDuration ttl, TDuration errorTtl);
        bool IsExpired(TDuration ttl);

        TEntry(const TKey& key, const TContext& context);
    };
    using TEntryPtr = TIntrusivePtr<TEntry>;

    const TAuthCacheConfigPtr Config_;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TKey, TEntryPtr> Cache_;

    virtual TFuture<TValue> DoGet(const TKey& key, const TContext& context) noexcept = 0;
    void TryErase(const TWeakPtr<TEntry>& weakEntry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#define AUTH_CACHE_INL_H_
#include "auth_cache-inl.h"
#undef AUTH_CACHE_INL_H_
