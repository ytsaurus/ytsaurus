#pragma once

#include "common.h"

#include <core/misc/error.h>
#include <core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TCacheBase;

template <class TKey, class TValue, class THash = ::hash<TKey> >
class TCacheValueBase
    : public virtual TRefCounted
{
public:
    virtual ~TCacheValueBase();

    const TKey& GetKey() const;

protected:
    explicit TCacheValueBase(const TKey& key);

private:
    typedef TCacheBase<TKey, TValue, THash> TCache;
    friend class TCacheBase<TKey, TValue, THash>;

    TIntrusivePtr<TCache> Cache;
    TKey Key;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = hash<TKey> >
class TCacheBase
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef TErrorOr<TValuePtr> TValuePtrOrError;
    typedef TFuture<TValuePtrOrError> TAsyncValuePtrOrErrorResult;
    typedef TPromise<TValuePtrOrError> TAsyncValuePtrOrErrorPromise;

    void Clear();
    int GetSize() const;
    TValuePtr Find(const TKey& key);
    std::vector<TValuePtr> GetAll();

protected:
    TSpinLock SpinLock;

    class TInsertCookie
    {
    public:
        explicit TInsertCookie(const TKey& key);
        ~TInsertCookie();

        inline TKey GetKey() const;
        inline TAsyncValuePtrOrErrorResult GetValue() const;
        inline bool IsActive() const;

        void Cancel(const TError& error);
        void EndInsert(TValuePtr value);

    private:
        friend class TCacheBase;

        TKey Key;
        TIntrusivePtr<TCacheBase> Cache;
        TAsyncValuePtrOrErrorResult ValueOrError;
        bool Active;

    };

    TCacheBase();

    TAsyncValuePtrOrErrorResult Lookup(const TKey& key);
    bool BeginInsert(TInsertCookie* cookie);
    void Touch(const TKey& key);
    bool Remove(const TKey& key);

    //! Called under #SpinLock.
    virtual bool IsTrimNeeded() const = 0;

    //! Must acquire #SpinLock if needed.
    virtual void OnAdded(TValue* value);

    //! Must acquire #SpinLock if needed.
    virtual void OnRemoved(TValue* value);

private:
    friend class TCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : TIntrusiveListItem<TItem>
    {
        TItem()
            : ValueOrError(NewPromise<TValuePtrOrError>())
        { }

        explicit TItem(const TValuePtr& value)
            : ValueOrError(MakePromise(TValuePtrOrError(value)))
        { }

        explicit TItem(TValuePtr&& value)
            : ValueOrError(MakePromise(TValuePtrOrError(std::move(value))))
        { }

        TAsyncValuePtrOrErrorPromise ValueOrError;
    };

    typedef yhash_map<TKey, TValue*, THash> TValueMap;
    typedef yhash_map<TKey, TItem*, THash> TItemMap;
    typedef TIntrusiveListWithAutoDelete<TItem, TDelete> TItemList;

    TValueMap ValueMap;
    TItemMap ItemMap;
    TItemList LruList;
    int ItemMapSize;

    void EndInsert(TValuePtr value, TInsertCookie* cookie);
    void CancelInsert(const TKey& key, const TError& error);
    void Touch(TItem* item); // thread-unsafe
    void Unregister(const TKey& key);
    void TrimIfNeeded(); // thread-unsafe

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = ::hash<TKey> >
class TSizeLimitedCache
    : public TCacheBase<TKey, TValue, THash>
{
protected:
    explicit TSizeLimitedCache(int maxSize);

    virtual bool IsTrimNeeded() const;

private:
    int MaxSize;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = ::hash<TKey> >
class TWeightLimitedCache
    : public TCacheBase<TKey, TValue, THash>
{
protected:
    explicit TWeightLimitedCache(i64 maxWeight);

    virtual i64 GetWeight(TValue* value) const = 0;

    virtual void OnAdded(TValue* value) override;
    virtual void OnRemoved(TValue* value) override;

    virtual bool IsTrimNeeded() const override;

private:
    i64 TotalWeight;
    i64 MaxWeight;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CACHE_INL_H_
#include "cache-inl.h"
#undef CACHE_INL_H_
