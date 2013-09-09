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
    ItemMapSize = 0;

    LruList.Clear();
}

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TCacheBase()
    : ItemMapSize(0)
{ }

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TValuePtr
TCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ValueMap.find(key);
    if (it == ValueMap.end()) {
        return nullptr;
    }
    return TRefCounted::DangerousGetPtr<TValue>(it->second);
}

template <class TKey, class TValue, class THash>
std::vector<typename TCacheBase<TKey, TValue, THash>::TValuePtr>
TCacheBase<TKey, TValue, THash>::GetAll()
{
    std::vector<TValuePtr> result;
    TGuard<TSpinLock> guard(SpinLock);
    result.reserve(ValueMap.size());
    FOREACH (const auto& pair, ValueMap) {
        auto value = TRefCounted::DangerousGetPtr<TValue>(pair.second);
        if (value) {
            result.push_back(value);
        }
    }
    return result;
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncValuePtrOrErrorResult
TCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    while (true) {
        {
            TGuard<TSpinLock> guard(SpinLock);
            auto itemIt = ItemMap.find(key);
            if (itemIt != ItemMap.end()) {
                auto* item = itemIt->second;
                Touch(item);
                return item->ValueOrError;
            }

            auto valueIt = ValueMap.find(key);
            if (valueIt == ValueMap.end()) {
                return TAsyncValuePtrOrErrorResult();
            }

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem(value);
                // This holds an extra reference to the promise state...
                auto valueOrError = item->ValueOrError;
                
                ItemMap.insert(std::make_pair(key, item));
                ++ItemMapSize;
                
                LruList.PushFront(item);

                guard.Release();

                // ...since the item can be dead at this moment.
                TrimIfNeeded();

                return valueOrError;
            }
        }

        // Back off.
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::BeginInsert(TInsertCookie* cookie)
{
    YCHECK(!cookie->Active);
    auto key = cookie->GetKey();

    while (true) {
        {
            TGuard<TSpinLock> guard(SpinLock);

            auto itemIt = ItemMap.find(key);
            if (itemIt != ItemMap.end()) {
                auto* item = itemIt->second;
                cookie->ValueOrError = item->ValueOrError;
                return false;
            }

            auto valueIt = ValueMap.find(key);
            if (valueIt == ValueMap.end()) {
                auto* item = new TItem();

                YCHECK(ItemMap.insert(std::make_pair(key, item)).second);
                ++ItemMapSize;

                cookie->ValueOrError = item->ValueOrError;
                cookie->Active = true;
                cookie->Cache = this;

                return true;
            }

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem(value);

                YCHECK(ItemMap.insert(std::make_pair(key, item)).second);
                ++ItemMapSize;

                LruList.PushFront(item);

                cookie->ValueOrError = item->ValueOrError;

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
    YCHECK(cookie->Active);

    auto key = value->GetKey();

    TAsyncValuePtrOrErrorPromise valueOrError;
    {
        TGuard<TSpinLock> guard(SpinLock);

        YCHECK(!value->Cache);
        value->Cache = this;

        auto it = ItemMap.find(key);
        YCHECK(it != ItemMap.end());

        auto* item = it->second;
        valueOrError = item->ValueOrError;

        YCHECK(ValueMap.insert(std::make_pair(key, ~value)).second);
    }

    valueOrError.Set(value);

    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = ItemMap.find(key);
        if (it != ItemMap.end()) {
            auto* item = it->second;
            LruList.PushFront(item);
        }
    }

    OnAdded(~value);
    TrimIfNeeded();
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    TAsyncValuePtrOrErrorPromise valueOrError;
    {
        TGuard<TSpinLock> guard(SpinLock);

        auto it = ItemMap.find(key);
        YCHECK(it != ItemMap.end());

        auto* item = it->second;
        valueOrError = item->ValueOrError;

        ItemMap.erase(it);
        --ItemMapSize;

        delete item;
    }

    valueOrError.Set(error);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    
    YCHECK(ItemMap.find(key) == ItemMap.end());   
    YCHECK(ValueMap.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = ItemMap.find(key);
    YCHECK(it != ItemMap.end());
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
    maybeValueOrError = item->ValueOrError.TryGet();

    YCHECK(maybeValueOrError);
    YCHECK(maybeValueOrError->IsOK());
    auto value = maybeValueOrError->GetValue();

    ItemMap.erase(it);
    --ItemMapSize;

    if (!item->Empty()) {
        item->Unlink();
    }

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TCacheValueBase::~TCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    OnRemoved(~value);

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
void TCacheBase<TKey, TValue, THash>::OnAdded(TValue* value)
{
    UNUSED(value);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::OnRemoved(TValue* value)
{
    UNUSED(value);
}

template <class TKey, class TValue, class THash>
int TCacheBase<TKey, TValue, THash>::GetSize() const
{
    return ItemMapSize;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TrimIfNeeded()
{
    while (true) {
        TGuard<TSpinLock> guard(SpinLock);

        if (LruList.Empty() || !IsTrimNeeded())
            return;

        auto* item = LruList.PopBack();

        auto maybeValueOrError = item->ValueOrError.TryGet();

        YCHECK(maybeValueOrError);
        YCHECK(maybeValueOrError->IsOK());

        auto value = maybeValueOrError->GetValue();

        YCHECK(ItemMap.erase(value->GetKey()) == 1);
        --ItemMapSize;

        guard.Release();

        OnRemoved(~value);

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
TKey TCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key;
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncValuePtrOrErrorResult
TCacheBase<TKey, TValue, THash>::TInsertCookie::GetValue() const
{
    return ValueOrError;
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active;
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
    YCHECK(Active);
    Cache->EndInsert(value, this);
    Active = false;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSizeLimitedCache<TKey, TValue, THash>::TSizeLimitedCache(int maxSize)
    : MaxSize(maxSize)
{ }

template <class TKey, class TValue, class THash>
bool TSizeLimitedCache<TKey, TValue, THash>::IsTrimNeeded() const
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
    TGuard<TSpinLock> guard(this->SpinLock);
    TotalWeight += GetWeight(value);
}

template <class TKey, class TValue, class THash>
void TWeightLimitedCache<TKey, TValue, THash>::OnRemoved(TValue* value)
{
    TGuard<TSpinLock> guard(this->SpinLock);
    TotalWeight -= GetWeight(value);
}

template <class TKey, class TValue, class THash>
bool TWeightLimitedCache<TKey, TValue, THash>::IsTrimNeeded() const
{
    return TotalWeight > MaxWeight;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
