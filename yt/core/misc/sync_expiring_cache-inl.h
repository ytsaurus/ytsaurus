#pragma once
#ifndef SYNC_EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_expiring_cache.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TSyncExpiringCache<TKey, TValue>::TSyncExpiringCache(
    TCallback<TValue(TKey)> calculateValueAction,
    TDuration expirationTimeout)
    : TAsyncExpiringCache<TKey, TValue>(MakeConfig(expirationTimeout))
    , CalculateValueAction_(calculateValueAction)
{ }

template <class TKey, class TValue>
TValue TSyncExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    auto future = TAsyncExpiringCache<TKey, TValue>::Get(key);
    return future.Get().ValueOrThrow();
}

template <class TKey, class TValue>
TFuture<TValue> TSyncExpiringCache<TKey, TValue>::DoGet(const TKey& key)
{
    return MakeFuture(CalculateValueAction_(key));
}

template <class TKey, class TValue>
TExpiringCacheConfigPtr TSyncExpiringCache<TKey, TValue>::MakeConfig(TDuration expirationTimeout)
{
    auto result = New<TExpiringCacheConfig>();
    result->ExpireAfterAccessTime = expirationTimeout;
    result->ExpireAfterFailedUpdateTime = expirationTimeout;
    result->ExpireAfterSuccessfulUpdateTime = expirationTimeout;
    result->RefreshTime = expirationTimeout;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
