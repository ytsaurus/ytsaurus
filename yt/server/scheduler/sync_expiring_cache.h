#pragma once

#include <yt/core/misc/expiring_cache.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TSyncExpiringCache
    : public TExpiringCache<TKey, TValue>
{
public:
    TSyncExpiringCache(
        TCallback<TValue(TKey)> calculateValueAction,
        TDuration expirationTimeout)
        : TExpiringCache<TKey, TValue>(MakeConfig(expirationTimeout))
        , CalculateValueAction_(calculateValueAction)
    { }

    TValue Get(const TKey& key)
    {
        auto future = TExpiringCache<TKey, TValue>::Get(key);
        YCHECK(future.IsSet());
        return future.Get().ValueOrThrow();
    }

protected:
    TFuture<TValue> DoGet(const TKey& key) override
    {
        return MakeFuture(CalculateValueAction_(key));
    }

private:
    const TCallback<TValue(TKey)> CalculateValueAction_;

    TExpiringCacheConfigPtr MakeConfig(TDuration expirationTimeout)
    {
        auto result = New<TExpiringCacheConfig>();
        result->ExpireAfterAccessTime = expirationTimeout;
        result->ExpireAfterFailedUpdateTime = expirationTimeout;
        result->ExpireAfterSuccessfulUpdateTime = expirationTimeout;
        result->RefreshTime = expirationTimeout;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
