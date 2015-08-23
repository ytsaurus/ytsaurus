#pragma once

#include "public.h"

#include <core/concurrency/rw_spinlock.h>

#include <core/profiling/profiler.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TSyncSlruCacheBase;

template <class TKey, class TValue, class THash = ::hash<TKey>>
class TSyncCacheValueBase
    : public virtual TRefCounted
{
public:
    const TKey& GetKey() const;

protected:
    explicit TSyncCacheValueBase(const TKey& key);

private:
    TKey Key_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = hash<TKey> >
class TSyncSlruCacheBase
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;

    int GetSize() const;
    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);

    bool TryInsert(TValuePtr value, TValuePtr* existingValue = nullptr);
    bool TryRemove(const TKey& key);
    bool TryRemove(TValuePtr value);
    void Clear();

protected:
    TSlruCacheConfigPtr Config_;


    explicit TSyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    virtual i64 GetWeight(const TValuePtr& value) const;
    virtual void OnAdded(const TValuePtr& value);
    virtual void OnRemoved(const TValuePtr& value);

private:
    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        explicit TItem(TValuePtr value)
            : Value(std::move(value))
        { }

        TValuePtr Value;
        bool Younger;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;

    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList_;
    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList_;

    yhash_map<TKey, TItem*, THash> ItemMap_;
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

    void PushToYounger(TItem* item);
    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);
    void Pop(TItem* item);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_CACHE_INL_H_
#include "sync_cache-inl.h"
#undef SYNC_CACHE_INL_H_
