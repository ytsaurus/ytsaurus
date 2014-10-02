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
    typedef TErrorOr<TValuePtr> TValuePtrOrError;
    typedef TFuture<TValuePtrOrError> TValuePtrOrErrorFuture;
    typedef TPromise<TValuePtrOrError> TValuePtrOrErrorPromise;

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
        TValuePtrOrErrorFuture GetValue() const;
        bool IsActive() const;

        void Cancel(const TError& error);
        void EndInsert(TValuePtr value);

    private:
        friend class TAsyncSlruCacheBase;

        TKey Key_;
        TIntrusivePtr<TAsyncSlruCacheBase> Cache_;
        TValuePtrOrErrorFuture ValueOrErrorPromise_;
        bool Active_;

        void Abort();

    };

    int GetSize() const;
    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);
    TValuePtrOrErrorFuture Lookup(const TKey& key);

    bool BeginInsert(TInsertCookie* cookie);
    bool Remove(const TKey& key);
    bool Remove(TValuePtr value);
    void Clear();

protected:
    TSlruCacheConfigPtr Config_;

    explicit TAsyncSlruCacheBase(TSlruCacheConfigPtr config);

    virtual i64 GetWeight(TValue* value) const = 0;

    virtual void OnAdded(TValue* value);
    virtual void OnRemoved(TValue* value);

private:
    friend class TAsyncCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TItem()
            : ValueOrErrorPromise(NewPromise<TValuePtrOrError>())
        { }

        explicit TItem(TValuePtr value)
            : ValueOrErrorPromise(MakePromise(TValuePtrOrError(value)))
            , Value(std::move(value))
        { }

        TValuePtrOrErrorPromise ValueOrErrorPromise;
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
