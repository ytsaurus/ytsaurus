#pragma once

#include "async_expiring_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): this should be redone
template <class TKey, class TValue>
class TSyncExpiringCache
    : public TExpiringCache<TKey, TValue>
{
public:
    TSyncExpiringCache(
        TCallback<TValue(TKey)> calculateValueAction,
        TDuration expirationTimeout);

    TValue Get(const TKey& key);

protected:
    virtual TFuture<TValue> DoGet(const TKey& key) override;

private:
    const TCallback<TValue(TKey)> CalculateValueAction_;

    TExpiringCacheConfigPtr MakeConfig(TDuration expirationTimeout);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_EXPIRING_CACHE_INL_H_
#include "sync_expiring_cache-inl.h"
#undef SYNC_EXPIRING_CACHE_INL_H_
