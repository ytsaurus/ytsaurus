#ifndef CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include cache.h"
#endif
#undef CACHE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
TKey TCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key;
}

template<class TKey, class TValue, class THash>
TCacheValueBase<TKey, TValue, THash>::TCacheValueBase(TKey key)
    : Key(key)
{
}

template<class TKey, class TValue, class THash>
NYT::TCacheValueBase<TKey, TValue, THash>::~TCacheValueBase()
{
    if (Cache.Get() != NULL) {
        Cache->Unregister(GetKey());
    }
}

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Clear()
{
    TGuard<TSpinLock> guard(SpinLock);
    ItemMap.clear();
    LruList.Clear();
    LruListSize = 0;
    // clear LRU
}

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TCacheBase()
    : LruListSize(0)
{}

template<class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncResultPtr
TCacheBase<TKey, TValue, THash>::Lookup(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);
    typename TItemMap::iterator itemIt = ItemMap.find(key);
    if (itemIt != ItemMap.end()) {
        TItem* item = itemIt->Second();
        Touch(item);
        return item->AsyncResult;
    }
    typename TValueMap::iterator valueIt = ValueMap.find(key);
    if (valueIt != ValueMap.end()) {
        TItem* item = new TItem();
        item->AsyncResult = new TAsyncResult<TValuePtr>();
        item->AsyncResult->Set(valueIt->Second());
        LruList.PushFront(item);
        ++LruListSize;
        ItemMap.insert(MakePair(key, item));
        Trim();
        return item->AsyncResult;
    }
    return NULL;
}

template<class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::BeginInsert(TInsertCookie* cookie)
{
    YASSERT(!cookie->Active);
    TGuard<TSpinLock> guard(SpinLock);
    TKey key = cookie->GetKey();
    typename TItemMap::iterator itemIt = ItemMap.find(key);
    if (itemIt != ItemMap.end()) {
        TItem* item = itemIt->Second();
        cookie->AsyncResult = item->AsyncResult;
        return false;
    }

    TItem* item = new TItem();
    item->AsyncResult = new TAsyncResult<TValuePtr>();
    ItemMap.insert(MakePair(key, item));
    cookie->AsyncResult = item->AsyncResult;

    typename TValueMap::iterator valueIt = ValueMap.find(key);
    if (valueIt != ValueMap.end()) {
        item->AsyncResult->Set(valueIt->Second());
        LruList.PushFront(item);
        ++LruListSize;
        Trim();
        return false;
    }

    cookie->Active = true;
    cookie->Cache = this;
    return true;
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::EndInsert(TValuePtr value, TInsertCookie* cookie)
{
    YASSERT(cookie->Active);
    TGuard<TSpinLock> guard(SpinLock);
    TKey key = value->GetKey();
    TItem* item = ItemMap.find(key)->Second();
    item->AsyncResult->Set(value);
    LruList.PushFront(item);
    ++LruListSize;
    YASSERT(value->Cache.Get() == NULL);
    YASSERT(ValueMap.find(key) == ValueMap.end());
    ValueMap.insert(MakePair(key, ~value));
    cookie->Active = false;
    Trim();
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::CancelInsert(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);
    typename TItemMap::iterator it = ItemMap.find(key);
    TItem* item = it->Second();
    item->AsyncResult->Set(NULL);
    ItemMap.erase(it);
    delete item;
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Unregister(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(ItemMap.find(key) == ItemMap.end());
    YASSERT(ValueMap.find(key) != ValueMap.end());
    ValueMap.erase(key);
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);
    TItem* item = ItemMap.find(key)->Second();
    Touch(item);
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(TItem* item)
{
    if (!item->Empty()) { 
        item->Unlink();
        LruList.PushFront(item);
    }
}

template<class TKey, class TValue, class THash>
i32 TCacheBase<TKey, TValue, THash>::GetSize() const
{
    return LruListSize;
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Trim()
{
    while (LruList.Size() > 0 && NeedTrim()) {
        TItem* item = LruList.PopBack();
        --LruListSize;
        TValuePtr value;
        bool got = item->AsyncResult->TryGet(&value);
        YASSERT(got); // TODO: verify?
        OnTrim(value);
        TKey key = value->GetKey();
        ItemMap.erase(key);
        delete item;
    }
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::OnTrim(TValuePtr value)
{
    UNUSED(value);
}

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(TKey key)
    : Key(key)
    , Active(false)
{}

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    if (Active) {
        Cache->CancelInsert(Key);
    }
}

template<class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncResultPtr
TCacheBase<TKey, TValue, THash>::TInsertCookie::GetAsyncResult() const
{
    return AsyncResult;
}

template<class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active;
}

template<class TKey, class TValue, class THash>
TKey TCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key;
}

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
TCapacityLimitedCache<TKey, TValue, THash>::TCapacityLimitedCache(i32 capacityLimit)
    : Capacity(capacityLimit)
{}

template<class TKey, class TValue, class THash>
bool TCapacityLimitedCache<TKey, TValue, THash>::NeedTrim() const
{
    return this->GetSize() > Capacity;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
