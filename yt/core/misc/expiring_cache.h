#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TExpiringCache
    : public virtual TRefCounted
{
public:
    typedef typename TFutureCombineTraits<TValue>::TCombined TCombinedValue;

    explicit TExpiringCache(TExpiringCacheConfigPtr config);

    TFuture<TValue> Get(const TKey& key);
    TFuture<TCombinedValue> Get(const std::vector<TKey>& keys);

    bool TryRemove(const TKey& key);

    void Clear();

protected:
    virtual TFuture<TValue> DoGet(const TKey& key) = 0;
    virtual TFuture<TCombinedValue> DoGetMany(const std::vector<TKey>& keys);

private:
    const TExpiringCacheConfigPtr Config_;

    struct TEntry
    {
        //! When this entry must be evicted.
        TInstant Deadline;
        //! Some latest known value (possibly not yet set).
        TPromise<TValue> Promise;
        //! Corresponds to a future probation request.
        NConcurrency::TDelayedExecutorCookie ProbationCookie;
        //! Corresponds to a future probation request.
        TFuture<TValue> ProbationFuture;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    yhash<TKey, TEntry> Map_;

    void SetResult(const TKey& key, const TErrorOr<TValue>& valueOrError);
    void InvokeGet(const TKey& key);
    void InvokeGetMany(const std::vector<TKey>& keys);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_
