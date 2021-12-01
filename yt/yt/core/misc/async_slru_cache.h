#pragma once

#include "public.h"
#include "cache_config.h"
#include "memory_usage_tracker.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TAsyncSlruCacheBase;

template <class TKey, class TValue, class THash = THash<TKey>>
class TAsyncCacheValueBase
    : public virtual TRefCounted
{
public:
    virtual ~TAsyncCacheValueBase();

    const TKey& GetKey() const;

    void UpdateWeight() const;

protected:
    explicit TAsyncCacheValueBase(const TKey& key);

private:
    using TCache = TAsyncSlruCacheBase<TKey, TValue, THash>;
    friend class TAsyncSlruCacheBase<TKey, TValue, THash>;

    TWeakPtr<TCache> Cache_;
    TKey Key_;
    typename TCache::TItem* Item_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class TItem, class TDerived>
class TAsyncSlruCacheListManager
{
public:
    void PushToYounger(TItem* item, i64 weight);

    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);

    void PopFromLists(TItem* item);

    void UpdateWeight(TItem* item, i64 weightDelta);

    TIntrusiveListWithAutoDelete<TItem, TDelete> TrimNoDelete();

    bool Touch(TItem* item);
    void DrainTouchBuffer();

    void Reconfigure(i64 capacity, double youngerSizeFraction);

    void SetTouchBufferCapacity(i64 touchBufferCapacity);

protected:
    TDerived* AsDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* AsDerived() const
    {
        return static_cast<const TDerived*>(this);
    }

    // Callbacks to be overloaded in derived classes.
    void OnYoungerUpdated(i64 deltaCount, i64 deltaWeight);
    void OnOlderUpdated(i64 deltaCount, i64 deltaWeight);

private:
    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList;
    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList;

    std::vector<TItem*> TouchBuffer;
    std::atomic<int> TouchBufferPosition = 0;

    size_t YoungerWeightCounter = 0;
    size_t OlderWeightCounter = 0;

    std::atomic<i64> Capacity;
    std::atomic<double> YoungerSizeFraction;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TAsyncSlruCacheBase
    : public virtual TRefCounted
{
public:
    using TValuePtr = TIntrusivePtr<TValue>;
    using TValueFuture = TFuture<TValuePtr>;
    using TValuePromise = TPromise<TValuePtr>;

    class TInsertCookie
    {
    public:
        TInsertCookie();
        explicit TInsertCookie(const TKey& key);
        TInsertCookie(TInsertCookie&& other);
        TInsertCookie(const TInsertCookie& other) = delete;
        ~TInsertCookie();

        TInsertCookie& operator = (TInsertCookie&& other);
        TInsertCookie& operator = (const TInsertCookie& other) = delete;

        const TKey& GetKey() const;
        TValueFuture GetValue() const;
        bool IsActive() const;

        void Cancel(const TError& error);
        void EndInsert(TValuePtr value);

    private:
        friend class TAsyncSlruCacheBase;

        TKey Key_;
        TIntrusivePtr<TAsyncSlruCacheBase> Cache_;
        TValueFuture ValueFuture_;
        std::atomic<bool> Active_;

        TInsertCookie(
            const TKey& key,
            TIntrusivePtr<TAsyncSlruCacheBase> cache,
            TValueFuture valueFuture,
            bool active);

        void Abort();
    };

    // NB: Shards store reference to the cache, so the cache cannot be simply copied or moved.
    TAsyncSlruCacheBase(const TAsyncSlruCacheBase&) = delete;
    TAsyncSlruCacheBase(TAsyncSlruCacheBase&&) = delete;
    TAsyncSlruCacheBase& operator=(const TAsyncSlruCacheBase&) = delete;
    TAsyncSlruCacheBase& operator=(TAsyncSlruCacheBase&&) = delete;

    int GetSize() const;
    i64 GetCapacity() const;

    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);
    TValueFuture Lookup(const TKey& key);
    void Touch(const TValuePtr& value);

    TInsertCookie BeginInsert(const TKey& key);
    void TryRemove(const TKey& key, bool forbidResurrection = false);
    void TryRemoveValue(const TValuePtr& value, bool forbidResurrection = false);

    void UpdateWeight(const TKey& key);
    void UpdateWeight(const TValuePtr& value);

    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config);

protected:
    const TSlruCacheConfigPtr Config_;

    explicit TAsyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler = {});

    // Called once when the value is inserted to the cache.
    // If item weight ever changes, UpdateWeight() should be called to apply the changes.
    virtual i64 GetWeight(const TValuePtr& value) const;

    virtual void OnAdded(const TValuePtr& value);
    virtual void OnRemoved(const TValuePtr& value);

    virtual bool IsResurrectionSupported() const;

private:
    friend class TAsyncCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TItem();
        explicit TItem(TValuePtr value);

        TValueFuture GetValueFuture() const;

        TValuePromise ValuePromise;
        TValuePtr Value;
        i64 CachedWeight;
        //! Counter for accurate calculation of AsyncHitWeight.
        //! It can be updated concurrently under the ReadLock.
        std::atomic<int> AsyncHitCount = 0;
        bool Younger = false;
    };

    class TShard : public TAsyncSlruCacheListManager<TItem, TShard>
    {
    public:
        YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, SpinLock);

        THashMap<TKey, TValue*, THash> ValueMap;
        THashMap<TKey, TItem*, THash> ItemMap;

        TAsyncSlruCacheBase* Parent;

        //! Trims the lists and releases the guard. Returns the list of evicted items.
        std::vector<TValuePtr> Trim(
            NConcurrency::TSpinlockWriterGuard<NConcurrency::TReaderWriterSpinLock>& guard);

    protected:
        void OnYoungerUpdated(i64 deltaCount, i64 deltaWeight);
        void OnOlderUpdated(i64 deltaCount, i64 deltaWeight);

        friend class TAsyncSlruCacheListManager<TItem, TShard>;
    };

    friend class TShard;

    std::unique_ptr<TShard[]> Shards_;

    std::atomic<int> Size_ = 0;
    std::atomic<i64> Capacity_;

    /*
     * Every request counts to one of the following metric types:
     *
     * SyncHit* - Item is present in the cache and contains the value.
     *
     * AsyncHit* - Item is present in the cache and contains the value future.
     * Caller should wait till the concurrent request set the value.
     *
     * Missed* - Item is missing in the cache and should be requested.
     *
     * Hit/Missed counters are updated immediately, while the update of
     * all Weight* metrics can be delayed till the EndInsert call,
     * because we do not know the weight of the object before it arrives.
     */
    NProfiling::TCounter SyncHitWeightCounter_;
    NProfiling::TCounter AsyncHitWeightCounter_;
    NProfiling::TCounter MissedWeightCounter_;
    NProfiling::TCounter SyncHitCounter_;
    NProfiling::TCounter AsyncHitCounter_;
    NProfiling::TCounter MissedCounter_;
    std::atomic<i64> YoungerWeightCounter_ = 0;
    std::atomic<i64> OlderWeightCounter_ = 0;
    std::atomic<i64> YoungerSizeCounter_ = 0;
    std::atomic<i64> OlderSizeCounter_ = 0;

    TShard* GetShardByKey(const TKey& key) const;

    TValueFuture DoLookup(TShard* shard, const TKey& key);

    void DoTryRemove(const TKey& key, const TValuePtr& value, bool forbidResurrection);

    //! Calls OnAdded on OnRemoved for the values evicted with Trim(). If the trim was caused by insertion, then
    //! insertedValue must be the value, insertion of which caused trim. Otherwise, insertedValue must be nullptr.
    void NotifyOnTrim(const std::vector<TValuePtr>& evictedValues, const TValuePtr& insertedValue);

    void EndInsert(TValuePtr value);
    void CancelInsert(const TKey& key, const TError& error);
    void Unregister(const TKey& key);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TMemoryTrackingAsyncSlruCacheBase
    : public TAsyncSlruCacheBase<TKey, TValue, THash>
{
public:
    explicit TMemoryTrackingAsyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler = {});
    ~TMemoryTrackingAsyncSlruCacheBase();

    void Reconfigure(const TSlruCacheDynamicConfigPtr& config) override;

protected:
    using TValuePtr = typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr;

    void OnAdded(const TValuePtr& value) override;
    void OnRemoved(const TValuePtr& value) override;

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_SLRU_CACHE_INL_H_
#include "async_slru_cache-inl.h"
#undef ASYNC_SLRU_CACHE_INL_H_
