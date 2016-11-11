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
        : public TRefCounted
    {
        //! When this entry must be evicted with respect to access timeout.
        TInstant AccessDeadline;
        //! When this entry must be evicted with respect to update timeout.
        TInstant UpdateDeadline;
        //! Some latest known value (possibly not yet set).
        TPromise<TValue> Promise;
        //! Corresponds to a future probation request.
        NConcurrency::TDelayedExecutorCookie ProbationCookie;

        //! Check that entry is expired with respect to either access or update.
        bool Expired(const TInstant& now) const;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    yhash<TKey, TIntrusivePtr<TEntry>> Map_;

    void SetResult(const TWeakPtr<TEntry>& entry, const TKey& key, const TErrorOr<TValue>& valueOrError);
    void InvokeGetMany(const std::vector<TWeakPtr<TEntry>>& entries, const std::vector<TKey>& keys);
    void InvokeGet(const TWeakPtr<TEntry>& entry, const TKey& key);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_

