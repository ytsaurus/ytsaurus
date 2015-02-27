#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/concurrency/rw_spinlock.h>

#include <core/profiling/timing.h>

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

    TIntrusivePtr<TCache> Cache_;
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
        TValueFuture ValuePromise_;
        bool Active_;

        void Abort();

    };

    int GetSize() const;
    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);
    TValueFuture Lookup(const TKey& key);

    bool BeginInsert(TInsertCookie* cookie);
    bool TryRemove(const TKey& key);
    bool TryRemove(TValuePtr value);
    void Clear();

protected:
    TSlruCacheConfigPtr Config_;

    explicit TAsyncSlruCacheBase(TSlruCacheConfigPtr config);

    virtual i64 GetWeight(TValue* value) const;

    virtual void OnAdded(TValue* value);
    virtual void OnRemoved(TValue* value);

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
        NProfiling::TCpuInstant NextTouchInstant = 0;
    };

    NConcurrency::TReaderWriterSpinLock SpinLock_;

    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList_;
    i64 YoungerWeight_ = 0;

    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList_;
    i64 OlderWeight_ = 0;

    yhash_map<TKey, TValue*, THash> ValueMap_;

    yhash_map<TKey, TItem*, THash> ItemMap_;
    volatile int ItemMapSize_ = 0; // used by GetSize


    void EndInsert(TValuePtr value, TInsertCookie* cookie);
    void CancelInsert(const TKey& key, const TError& error);
    static bool CanTouch(TItem* item);
    void Touch(const TKey& key);
    void Unregister(const TKey& key);
    void PushToYounger(TItem* item);
    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);
    void Pop(TItem* item);
    void TrimIfNeeded();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_CACHE_INL_H_
#include "async_cache-inl.h"
#undef ASYNC_CACHE_INL_H_
