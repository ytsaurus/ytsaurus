#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TAsyncExpiringCache
    : public virtual TRefCounted
{
public:
    struct TExtendedGetResult
    {
        TFuture<TValue> Future;
        bool RequestInitialized;
    };

    explicit TAsyncExpiringCache(
        TAsyncExpiringCacheConfigPtr config,
        NProfiling::TProfiler profiler = {});

    TFuture<TValue> Get(const TKey& key);
    TExtendedGetResult GetExtended(const TKey& key);
    TFuture<std::vector<TErrorOr<TValue>>> Get(const std::vector<TKey>& keys);

    std::optional<TErrorOr<TValue>> Find(const TKey& key);
    std::vector<std::optional<TErrorOr<TValue>>> Find(const std::vector<TKey>& keys);

    void Invalidate(const TKey& key);
    void Set(const TKey& key, TErrorOr<TValue> valueOrError);

    void Clear();

protected:
    const TAsyncExpiringCacheConfigPtr Config_;

    virtual TFuture<TValue> DoGet(
        const TKey& key,
        bool isPeriodicUpdate) noexcept = 0;
    virtual TFuture<std::vector<TErrorOr<TValue>>> DoGetMany(
        const std::vector<TKey>& keys,
        bool isPeriodicUpdate) noexcept;

    //! Called under write lock.
    virtual void OnAdded(const TKey& key) noexcept;

    //! Called under write lock.
    virtual void OnRemoved(const TKey& key) noexcept;

    //! Called under read or write lock.
    virtual void OnHit(const TKey& key) noexcept;

private:
    const NProfiling::TProfiler Profiler_;

    struct TEntry
        : public TRefCounted
    {
        //! When this entry must be evicted with respect to access timeout.
        std::atomic<NProfiling::TCpuInstant> AccessDeadline;

        //! When this entry must be evicted with respect to update timeout.
        NProfiling::TCpuInstant UpdateDeadline;

        //! Some latest known value (possibly not yet set).
        TPromise<TValue> Promise;

        //! Uncancelable version of #Promise.
        TFuture<TValue> Future;

        //! Corresponds to a future probation request.
        NConcurrency::TDelayedExecutorCookie ProbationCookie;

        //! Constructs a fresh entry.
        explicit TEntry(NProfiling::TCpuInstant accessDeadline);

        //! Check that entry is expired with respect to either access or update.
        bool IsExpired(NProfiling::TCpuInstant now) const;

    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    THashMap<TKey, TIntrusivePtr<TEntry>> Map_;

    NProfiling::TMonotonicCounter HitCounter_{"/hit"};
    NProfiling::TMonotonicCounter MissedCounter_{"/missed"};
    NProfiling::TSimpleGauge SizeCounter_{"/size"};

    void SetResult(
        const TWeakPtr<TEntry>& entry,
        const TKey& key,
        const TErrorOr<TValue>& valueOrError,
        bool isPeriodicUpdate);

    void InvokeGetMany(
        const std::vector<TWeakPtr<TEntry>>& entries,
        const std::vector<TKey>& keys,
        bool isPeriodicUpdate);

    void InvokeGet(
        const TWeakPtr<TEntry>& entry,
        const TKey& key,
        bool isPeriodicUpdate);

    bool TryEraseExpired(
        const TWeakPtr<TEntry>& weakEntry,
        const TKey& key);

    void UpdateAll();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "async_expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_

