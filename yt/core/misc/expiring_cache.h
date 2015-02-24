#pragma once

#include "public.h"

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/rw_spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TExpiringCache
    : public TRefCounted
{
public:
    explicit TExpiringCache(TExpiringCacheConfigPtr config);

    TFuture<TValue> Get(const TKey& key);

    void Clear();

protected:
    virtual void DoGet(const TKey& key) = 0;
    void DoSet(const TKey& key, const TErrorOr<TValue>& valueOrError);

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
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    yhash<TKey, TEntry> Map_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_
