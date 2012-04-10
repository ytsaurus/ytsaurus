#ifndef CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include cache.h"
#endif
#undef CACHE_INL_H_

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TKey TCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key;
}

template <class TKey, class TValue, class THash>
TCacheValueBase<TKey, TValue, THash>::TCacheValueBase(const TKey& key)
    : Key(key)
{ }

template <class TKey, class TValue, class THash>
NYT::TCacheValueBase<TKey, TValue, THash>::~TCacheValueBase()
{
    if (Cache) {
        Cache->Unregister(GetKey());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Clear()
{
    TGuard<TSpinLock> guard(SpinLock);
    ItemMap.clear();
    LruList.Clear();
    Size = 0;
}

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TCacheBase()
    : Size(0)
{ }

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TValuePtr
TCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ValueMap.find(key);
    if (it == ValueMap.end()) {
        return NULL;
    }
    return TRefCounted::DangerousGetPtr<TValue>(it->second);
}

template <class TKey, class TValue, class THash>
yvector<typename TCacheBase<TKey, TValue, THash>::TValuePtr>
TCacheBase<TKey, TValue, THash>::GetAll()
{
    yvector<TValuePtr> result;
    TGuard<TSpinLock> guard(SpinLock);
    result.reserve(ValueMap.ysize());
    FOREACH (const auto& pair, ValueMap) {
        auto value = TRefCounted::DangerousGetPtr<TValue>(pair.second);
        if (value) {
            result.push_back(value);
        }
    }
    return result;
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncValuePtrOrError
TCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    while (true) {
        {
            TGuard<TSpinLock> guard(SpinLock);
            auto itemIt = ItemMap.find(key);
            if (itemIt != ItemMap.end()) {
                TItem* item = itemIt->second;
                Touch(item);
                return item->AsyncResult;
            }

            auto valueIt = ValueMap.find(key);
            if (valueIt == ValueMap.end()) {
                return TAsyncValuePtrOrError();
            }

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem();
                // This holds an extra reference to the promise state...
                auto asyncResult = item->AsyncResult;
                LruList.PushFront(item);
                ++Size;
                ItemMap.insert(MakePair(key, item));
                guard.Release();

                // ...since the item can be dead at this moment.
                TrimIfNeeded();

                return asyncResult;
            }
        }

        // Back off.
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::BeginInsert(TInsertCookie* cookie)
{
    YASSERT(!cookie->Active);
    auto key = cookie->GetKey();

    while (true) {
        {
            TGuard<TSpinLock> guard(SpinLock);

            auto itemIt = ItemMap.find(key);
            if (itemIt != ItemMap.end()) {
                auto* item = itemIt->second;
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

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem(value);
                ItemMap.insert(MakePair(key, item));

                LruList.PushFront(item);
                ++Size;

                cookie->AsyncResult = item->AsyncResult;

                guard.Release();

                TrimIfNeeded();

                return false;
            }
        }

        // Back off.
        // Hopefully the object we had just extracted will be destroyed soon
        // and thus vanish from ValueMap.
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::EndInsert(TValuePtr value, TInsertCookie* cookie)
{
    YASSERT(cookie->Active);

    auto key = value->GetKey();

    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(!value->Cache);
        value->Cache = this;

        auto it = ItemMap.find(key);
        YASSERT(it != ItemMap.end());

        auto* item = it->second;
        item->AsyncResult.Set(value);

        YVERIFY(ValueMap.insert(MakePair(key, ~value)).second);
    
        LruList.PushFront(item);
        ++Size;
        OnAdded(~value);
    }

    TrimIfNeeded();

    cookie->Active = false;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);

    auto it = ItemMap.find(key);
    YASSERT(it != ItemMap.end());
    
    auto* item = it->second;
    item->AsyncResult.Set(error);
    
    ItemMap.erase(it);
    
    delete item;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(ItemMap.find(key) == ItemMap.end());
    YVERIFY(ValueMap.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ItemMap.find(key);
    YASSERT(it != ItemMap.end());
    auto* item = it->second;
    Touch(item);
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::Remove(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ItemMap.find(key);
    if (it == ItemMap.end()) {
        return false;
    }

    auto* item = it->second;

    TNullable<TValuePtrOrError> maybeValueOrError;
    maybeValueOrError = item->AsyncResult.ToFuture().TryGet();

    YVERIFY(maybeValueOrError);
    YASSERT(maybeValueOrError->IsOK());
    auto value = maybeValueOrError->Value();

    item->Unlink();

    --Size;
    OnRemoved(~value);

    ItemMap.erase(it);

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TCacheValueBase::~TCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    delete item;

    return true;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(TItem* item)
{
    if (!item->Empty()) { 
        item->Unlink();
        LruList.PushFront(item);
    }
}

template <class TKey, class TValue, class THash>
i32 TCacheBase<TKey, TValue, THash>::GetSize() const
{
    return Size;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TrimIfNeeded()
{
    while (true) {
        TGuard<TSpinLock> guard(SpinLock);

        if (!NeedTrim())
            return;

        YASSERT(Size > 0);
        auto* item = LruList.PopBack();

        auto maybeValueOrError = item->AsyncResult.ToFuture().TryGet();

        YVERIFY(maybeValueOrError);
        YASSERT(maybeValueOrError->IsOK());

        auto value = maybeValueOrError->Value();

        --Size;
        OnRemoved(~value);

        ItemMap.erase(value->GetKey());

        guard.Release();

        delete item;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(const TKey& key)
    : Key(key)
    , Active(false)
{}

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    Cancel(TError("Cache item insertion aborted"));
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::Cancel(const TError& error)
{
    if (Active) {
        Cache->CancelInsert(Key, error);
        Active = false;
    }
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    YASSERT(Active);
    Cache->EndInsert(value, this);
    Active = false;
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncValuePtrOrError
TCacheBase<TKey, TValue, THash>::TInsertCookie::GetAsyncResult() const
{
    return AsyncResult;
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active;
}

template <class TKey, class TValue, class THash>
TKey TCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::OnAdded(TValue* value)
{
    UNUSED(value);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::OnRemoved(TValue* value)
{
    UNUSED(value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSizeLimitedCache<TKey, TValue, THash>::TSizeLimitedCache(i32 maxSize)
    : MaxSize(maxSize)
{ }

template <class TKey, class TValue, class THash>
bool TSizeLimitedCache<TKey, TValue, THash>::NeedTrim() const
{
    return this->GetSize() > MaxSize;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TWeightLimitedCache<TKey, TValue, THash>::TWeightLimitedCache(i64 maxWeight)
    : TotalWeight(0)
    , MaxWeight(maxWeight)
{ }

template <class TKey, class TValue, class THash>
void TWeightLimitedCache<TKey, TValue, THash>::OnAdded(TValue* value)
{
    TotalWeight += GetWeight(value);
}

template <class TKey, class TValue, class THash>
void TWeightLimitedCache<TKey, TValue, THash>::OnRemoved(TValue* value)
{
    TotalWeight -= GetWeight(value);
}

template <class TKey, class TValue, class THash>
bool TWeightLimitedCache<TKey, TValue, THash>::NeedTrim() const
{
    return TotalWeight > MaxWeight;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
