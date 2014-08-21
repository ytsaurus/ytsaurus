#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TSlruCacheBase;

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
    typedef TSlruCacheBase<TKey, TValue, THash> TCache;
    friend class TSlruCacheBase<TKey, TValue, THash>;

    TIntrusivePtr<TCache> Cache_;
    TKey Key_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = hash<TKey> >
class TSlruCacheBase
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
    TSlruCacheConfigPtr Config_;

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

        TKey GetKey() const;
        TAsyncValuePtrOrErrorResult GetValue() const;
        bool IsActive() const;

        void Cancel(const TError& error);
        void EndInsert(TValuePtr value);

    private:
        friend class TSlruCacheBase;

        TKey Key_;
        TIntrusivePtr<TSlruCacheBase> Cache_;
        TAsyncValuePtrOrErrorResult ValueOrError_;
        bool Active_;

        void Abort();

    };

    explicit TSlruCacheBase(TSlruCacheConfigPtr config);

    TAsyncValuePtrOrErrorResult Lookup(const TKey& key);
    bool BeginInsert(TInsertCookie* cookie);
    void Touch(const TKey& key);
    bool Remove(const TKey& key);
    bool Remove(TValuePtr value);

    virtual i64 GetWeight(TValue* value) const = 0;
    virtual void OnAdded(TValue* value);
    virtual void OnRemoved(TValue* value);

private:
    friend class TCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TItem()
            : ValueOrError(NewPromise<TValuePtrOrError>())
        { }

        explicit TItem(TValuePtr value)
            : ValueOrError(MakePromise(TValuePtrOrError(std::move(value))))
        { }

        TAsyncValuePtrOrErrorPromise ValueOrError;
        bool Younger;
    };

    typedef yhash_map<TKey, TValue*, THash> TValueMap;
    typedef yhash_map<TKey, TItem*, THash> TItemMap;
    typedef TIntrusiveListWithAutoDelete<TItem, TDelete> TItemList;

    TSpinLock SpinLock_;

    TItemList YoungerLruList_;
    i64 YoungerWeight_ = 0;

    TItemList OlderLruList_;
    i64 OlderWeight_ = 0;

    TValueMap ValueMap_;

    TItemMap ItemMap_;
    int ItemMapSize_ = 0;

    void EndInsert(TValuePtr value, TInsertCookie* cookie);
    void CancelInsert(const TKey& key, const TError& error);
    void Touch(TItem* item); // thread-unsafe
    void Unregister(const TKey& key);
    static TValuePtr GetValue(TItem* item);
    void PushToYounger(TItem* item, TValue* value);
    void MoveToYounger(TItem* item, TValue* value);
    void MoveToOlder(TItem* item, TValue* value);
    void Pop(TItem* item, TValue* value);
    void TrimIfNeeded();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CACHE_INL_H_
#include "cache-inl.h"
#undef CACHE_INL_H_
