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
{ }

template<class TKey, class TValue, class THash>
NYT::TCacheValueBase<TKey, TValue, THash>::~TCacheValueBase()
{
    if (~Cache != NULL) {
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
}

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TCacheBase()
    : LruListSize(0)
{ }

template<class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TFuturePtr
TCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    while (true) {
        TGuard<TSpinLock> guard(SpinLock);
        auto itemIt = ItemMap.find(key);
        if (itemIt != ItemMap.end()) {
            TItem* item = itemIt->Second();
            Touch(item);
            return item->AsyncResult;
        }

        auto valueIt = ValueMap.find(key);
        if (valueIt == ValueMap.end())
            return NULL;

        auto value = TRefCountedBase::DangerousGetPtr(valueIt->Second());
        if (~value != NULL) {
            auto* item = new TItem();
            item->AsyncResult = New< TFuture<TValuePtr> >();
            item->AsyncResult->Set(value);
            LruList.PushFront(item);
            ++LruListSize;
            ItemMap.insert(MakePair(key, item));
            guard.Release();

            Trim();
            
            return item->AsyncResult;
        }

        // Back off.
        guard.Release();
        ThreadYield();
    }
}

template<class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::BeginInsert(TInsertCookie* cookie)
{
    YASSERT(!cookie->Active);
    TKey key = cookie->GetKey();

    while (true) {
        TGuard<TSpinLock> guard(SpinLock);

        auto itemIt = ItemMap.find(key);
        if (itemIt != ItemMap.end()) {
            auto* item = itemIt->Second();
            cookie->AsyncResult = item->AsyncResult;
            return false;
        }

        auto valueIt = ValueMap.find(key);
        if (valueIt == ValueMap.end()) {
            auto* item = new TItem();
            ItemMap.insert(MakePair(key, item));

            cookie->AsyncResult = item->AsyncResult;
            cookie->Active = true;
            cookie->Cache = this;

            return true;
        }

        auto value = TRefCountedBase::DangerousGetPtr(valueIt->Second());
        if (~value != NULL) {
            auto* item = new TItem(value);
            ItemMap.insert(MakePair(key, item));

            LruList.PushFront(item);
            ++LruListSize;

            cookie->AsyncResult = item->AsyncResult;

            guard.Release();

            Trim();

            return false;
        }

        // Back off.
        // Hopefully the object we had just extracted will be destroyed soon
        // and thus vanish from ValueMap.
        guard.Release();
        ThreadYield();
    }
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::EndInsert(TValuePtr value, TInsertCookie* cookie)
{
    YASSERT(cookie->Active);

    auto key = value->GetKey();

    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(~value->Cache == NULL);
        value->Cache = this;

        auto it = ItemMap.find(key);
        YASSERT(it != ItemMap.end());

        auto* item = it->Second();
        item->AsyncResult->Set(value);

        YVERIFY(ValueMap.insert(MakePair(key, ~value)).second);
    
        LruList.PushFront(item);
        ++LruListSize;
    }

    Trim();

    cookie->Active = false;
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);

    auto it = ItemMap.find(key);
    YASSERT(it != ItemMap.end());
    
    auto* item = it->Second();
    item->AsyncResult->Set(NULL);
    
    ItemMap.erase(it);
    
    delete item;
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(ItemMap.find(key) == ItemMap.end());
    YVERIFY(ValueMap.erase(key) == 1);
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ItemMap.find(key);
    YASSERT(it != ItemMap.end());
    auto* item = it->Second();
    Touch(item);
}

template<class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::Remove(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ItemMap.find(key);
    if (it == ItemMap.end())
        return false;

    auto* item = it->Second();
    item->Unlink();
    --LruListSize;
    ItemMap.erase(it);

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TCacheValueBase::~TCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    delete item;

    return true;
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
    while (true) {
        TGuard<TSpinLock> guard(SpinLock);
        if (LruListSize == 0 || !NeedTrim())
            break;

        auto* item = LruList.PopBack();
        --LruListSize;

        TValuePtr value;
        YVERIFY(item->AsyncResult->TryGet(&value));

        TKey key = value->GetKey();
        ItemMap.erase(key);
        delete item;

        guard.Release();

        OnTrim(value);
    }
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::OnTrim(TValuePtr value)
{
    UNUSED(value);
}

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(const TKey& key)
    : Key(key)
    , Active(false)
{}

template<class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    Cancel();
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::Cancel()
{
    if (Active) {
        Cache->CancelInsert(Key);
        Active = false;
    }
}

template<class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    YASSERT(Active);
    Cache->EndInsert(value, this);
    Active = false;
}

template<class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TFuturePtr
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
