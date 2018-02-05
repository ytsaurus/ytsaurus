#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TAsyncSlruCacheBase;

template <class TKey, class TValue, class THash = ::hash<TKey>>
class TAsyncCacheValueBase
    : public virtual TRefCounted
{
public:
    virtual ~TAsyncCacheValueBase();

    const TKey& GetKey() const;

protected:
    explicit TAsyncCacheValueBase(const TKey& key);

private:
    typedef TAsyncSlruCacheBase<TKey, TValue, THash> TCache;
    friend class TAsyncSlruCacheBase<TKey, TValue, THash>;

    TWeakPtr<TCache> Cache_;
    TKey Key_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = hash<TKey> >
class TAsyncSlruCacheBase
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef TFuture<TValuePtr> TValueFuture;
    typedef TPromise<TValuePtr> TValuePromise;

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
        bool Active_;

        TInsertCookie(
            const TKey& key,
            TIntrusivePtr<TAsyncSlruCacheBase> cache,
            TValueFuture valueFuture,
            bool active);

        void Abort();

    };

    int GetSize() const;
    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);
    TValueFuture Lookup(const TKey& key);

    TInsertCookie BeginInsert(const TKey& key);
    bool TryRemove(const TKey& key);
    bool TryRemove(TValuePtr value);
    void Clear();

protected:
    TSlruCacheConfigPtr Config_;

    explicit TAsyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    virtual i64 GetWeight(const TValuePtr& value) const;

    virtual void OnAdded(const TValuePtr& value);
    virtual void OnRemoved(const TValuePtr& value);

private:
    friend class TAsyncCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TItem()
            : ValuePromise(NewPromise<TValuePtr>())
        { }

        explicit TItem(TValuePtr value)
            : ValuePromise(MakePromise(TValuePtr(value)))
            , Value(std::move(value))
        { }

        TValuePromise ValuePromise;
        TValuePtr Value;
        bool Younger;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;

    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList_;
    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList_;

    THashMap<TKey, TValue*, THash> ValueMap_;

    THashMap<TKey, TItem*, THash> ItemMap_;
    volatile int ItemMapSize_ = 0; // used by GetSize

    std::vector<TItem*> TouchBuffer_;
    std::atomic<int> TouchBufferPosition_ = {0};

    NProfiling::TProfiler Profiler;
    NProfiling::TSimpleCounter HitWeightCounter_;
    NProfiling::TSimpleCounter MissedWeightCounter_;
    NProfiling::TSimpleCounter YoungerWeightCounter_;
    NProfiling::TSimpleCounter OlderWeightCounter_;


    bool Touch(TItem* item);
    void DrainTouchBuffer();

    void Trim(NConcurrency::TWriterGuard& guard);

    void EndInsert(TValuePtr value, TInsertCookie* cookie);
    void CancelInsert(const TKey& key, const TError& error);
    void Unregister(const TKey& key);
    i64 PushToYounger(TItem* item);
    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);
    void Pop(TItem* item);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_CACHE_INL_H_
#include "async_cache-inl.h"
#undef ASYNC_CACHE_INL_H_
