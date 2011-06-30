#pragma once

#include "common.h"

#include "../actions/async_result.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
class TCacheBase;

template<class TKey, class TValue, class THash = hash<TKey> >
class TCacheValueBase
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TValue> TPtr;

    virtual ~TCacheValueBase();

    TKey GetKey() const;

protected:
    TCacheValueBase(TKey key);

private:
    typedef TCacheBase<TKey, TValue, THash> TCache;
    friend class TCacheBase<TKey, TValue, THash>;

    TIntrusivePtr<TCache> Cache;
    TKey Key;
};

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash = hash<TKey> >
class TCacheBase
    : public virtual TRefCountedBase
{
public:
    void Clear();
    i32 GetSize() const;

protected:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef TIntrusivePtr< TCacheBase<TKey, TValue, THash> > TPtr;
    typedef typename TAsyncResult<TValuePtr>::TPtr TAsyncResultPtr;

    class TInsertCookie
    {
        friend class TCacheBase;

        TKey Key;
        TPtr Cache;
        TAsyncResultPtr AsyncResult;
        bool Active;

    public:
        TInsertCookie(TKey key);
        ~TInsertCookie();

        TAsyncResultPtr GetAsyncResult() const;
        TKey GetKey() const;
        bool IsActive() const;
    };

    TCacheBase();

    TAsyncResultPtr Lookup(TKey key);
    bool BeginInsert(TInsertCookie* cookie);
    void EndInsert(TValuePtr value, TInsertCookie* cookie);
    void Touch(TKey key);

    virtual bool NeedTrim() const = 0;
    virtual void OnTrim(TValuePtr value);

private:
    friend class TCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TAsyncResultPtr AsyncResult;
    };

    TSpinLock SpinLock;

    typedef yhash_map<TKey, TValue*, THash> TValueMap;
    typedef yhash_map<TKey, TItem*, THash> TItemMap;
    typedef TIntrusiveListWithAutoDelete<TItem, TDelete> TItemList;

    TValueMap ValueMap;
    TItemMap ItemMap;
    TItemList LruList;
    i32 LruListSize;

    void CancelInsert(TKey key);
    void Touch(TItem* item); // thread-unsafe
    void Unregister(TKey key);
    void Trim(); // thread-unsafe
};

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash = hash<TKey> >
class TCapacityLimitedCache
    : public TCacheBase<TKey, TValue, THash>
{
protected:
    TCapacityLimitedCache(i32 capacity);
    virtual bool NeedTrim() const;

private:
    i32 Capacity;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CACHE_INL_H_
#include "cache-inl.h"
#undef CACHE_INL_H_
