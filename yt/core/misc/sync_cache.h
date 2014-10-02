#pragma once

#include "public.h"

#include <core/concurrency/rw_spinlock.h>

#include <core/profiling/timing.h>

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

    bool Insert(TValuePtr value, TValuePtr* existingValue = nullptr);
    bool Remove(const TKey& key);
    bool Remove(TValuePtr value);
    void Clear();

protected:
    TSlruCacheConfigPtr Config_;


    explicit TSyncSlruCacheBase(TSlruCacheConfigPtr config);

    virtual i64 GetWeight(TValue* value) const = 0;
    virtual void OnAdded(TValue* value);
    virtual void OnRemoved(TValue* value);

private:
    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        explicit TItem(TValuePtr value)
            : Value(std::move(value))
        { }

        TValuePtr Value;
        bool Younger;
        NProfiling::TCpuInstant NextTouchInstant = 0;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;

    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList_;
    i64 YoungerWeight_ = 0;

    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList_;
    i64 OlderWeight_ = 0;

    yhash_map<TKey, TItem*, THash> ItemMap_;
    volatile int ItemMapSize_ = 0; // used by GetSize


    static bool CanTouch(TItem* item);
    void Touch(const TKey& key);
    void PushToYounger(TItem* item);
    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);
    void Pop(TItem* item);
    void TrimIfNeeded();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_CACHE_INL_H_
#include "sync_cache-inl.h"
#undef SYNC_CACHE_INL_H_
