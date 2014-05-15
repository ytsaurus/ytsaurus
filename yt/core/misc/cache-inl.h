#ifndef CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include cache.h"
#endif
#undef CACHE_INL_H_

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
TCacheValueBase<TKey, TValue, THash>::TCacheValueBase(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TValue, class THash>
NYT::TCacheValueBase<TKey, TValue, THash>::~TCacheValueBase()
{
    if (Cache_) {
        Cache_->Unregister(Key_);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Clear()
{
    TGuard<TSpinLock> guard(SpinLock_);

    ItemMap_.clear();
    ItemMapSize_ = 0;

    LruList_.Clear();
}

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TCacheBase()
    : ItemMapSize_(0)
{ }

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TValuePtr
TCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = ValueMap_.find(key);
    if (it == ValueMap_.end()) {
        return nullptr;
    }
    return TRefCounted::DangerousGetPtr<TValue>(it->second);
}

template <class TKey, class TValue, class THash>
std::vector<typename TCacheBase<TKey, TValue, THash>::TValuePtr>
TCacheBase<TKey, TValue, THash>::GetAll()
{
    std::vector<TValuePtr> result;
    TGuard<TSpinLock> guard(SpinLock_);
    result.reserve(ValueMap_.size());
    for (const auto& pair : ValueMap_) {
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
            TGuard<TSpinLock> guard(SpinLock_);
            auto itemIt = ItemMap_.find(key);
            if (itemIt != ItemMap_.end()) {
                auto* item = itemIt->second;
                Touch(item);
                return item->ValueOrError;
            }

            auto valueIt = ValueMap_.find(key);
            if (valueIt == ValueMap_.end()) {
                return TAsyncValuePtrOrErrorResult();
            }

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem(value);
                // This holds an extra reference to the promise state...
                auto valueOrError = item->ValueOrError;
                
                ItemMap_.insert(std::make_pair(key, item));
                ++ItemMapSize_;
                
                LruList_.PushFront(item);

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
    YCHECK(!cookie->Active_);
    auto key = cookie->GetKey();

    while (true) {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            auto itemIt = ItemMap_.find(key);
            if (itemIt != ItemMap_.end()) {
                auto* item = itemIt->second;
                cookie->ValueOrError_ = item->ValueOrError;
                return false;
            }

            auto valueIt = ValueMap_.find(key);
            if (valueIt == ValueMap_.end()) {
                auto* item = new TItem();

                YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
                ++ItemMapSize_;

                cookie->ValueOrError_ = item->ValueOrError;
                cookie->Active_ = true;
                cookie->Cache_ = this;

                return true;
            }

            auto value = TRefCounted::DangerousGetPtr(valueIt->second);
            if (value) {
                auto* item = new TItem(value);

                YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
                ++ItemMapSize_;

                LruList_.PushFront(item);

                cookie->ValueOrError_ = item->ValueOrError;

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
    YCHECK(cookie->Active_);

    auto key = value->GetKey();

    TAsyncValuePtrOrErrorPromise valueOrError;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YCHECK(!value->Cache_);
        value->Cache_ = this;

        auto it = ItemMap_.find(key);
        YCHECK(it != ItemMap_.end());

        auto* item = it->second;
        valueOrError = item->ValueOrError;

        YCHECK(ValueMap_.insert(std::make_pair(key, value.Get())).second);
    }

    valueOrError.Set(value);

    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = ItemMap_.find(key);
        if (it != ItemMap_.end()) {
            auto* item = it->second;
            LruList_.PushFront(item);
        }
    }

    OnAdded(value.Get());
    TrimIfNeeded();
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    TAsyncValuePtrOrErrorPromise valueOrError;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto it = ItemMap_.find(key);
        YCHECK(it != ItemMap_.end());

        auto* item = it->second;
        valueOrError = item->ValueOrError;

        ItemMap_.erase(it);
        --ItemMapSize_;

        delete item;
    }

    valueOrError.Set(error);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock_);
    
    YCHECK(ItemMap_.find(key) == ItemMap_.end());   
    YCHECK(ValueMap_.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = ItemMap_.find(key);
    YCHECK(it != ItemMap_.end());
    auto* item = it->second;
    Touch(item);
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::Remove(const TKey& key)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return false;
    }

    auto* item = it->second;

    TNullable<TValuePtrOrError> maybeValueOrError;
    maybeValueOrError = item->ValueOrError.TryGet();

    YCHECK(maybeValueOrError);
    YCHECK(maybeValueOrError->IsOK());
    auto value = maybeValueOrError->Value();

    ItemMap_.erase(it);
    --ItemMapSize_;

    if (!item->Empty()) {
        item->Unlink();
    }

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TCacheValueBase::~TCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    OnRemoved(value.Get());

    delete item;

    return true;
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::Remove(TValuePtr value)
{
    TGuard<TSpinLock> guard(SpinLock_);

    auto valueIt = ValueMap_.find(value->GetKey());
    if (valueIt == ValueMap_.end() || valueIt->second != value) {
        return false;
    }
    ValueMap_.erase(valueIt);

    auto itemIt = ItemMap_.find(value->GetKey());
    if (itemIt != ItemMap_.end()) {
        auto* item = itemIt->second;
        ItemMap_.erase(itemIt);
        --ItemMapSize_;

        if (!item->Empty()) {
            item->Unlink();
        }

        delete item;
    }

    value->Cache_.Reset();

    guard.Release();

    OnRemoved(value.Get());

    return true;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::Touch(TItem* item)
{
    if (!item->Empty()) {
        item->Unlink();
        LruList_.PushFront(item);
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
    return ItemMapSize_;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TrimIfNeeded()
{
    while (true) {
        TGuard<TSpinLock> guard(SpinLock_);

        if (LruList_.Empty() || !IsTrimNeeded())
            return;

        auto* item = LruList_.PopBack();

        auto maybeValueOrError = item->ValueOrError.TryGet();

        YCHECK(maybeValueOrError);
        YCHECK(maybeValueOrError->IsOK());

        auto value = maybeValueOrError->Value();

        YCHECK(ItemMap_.erase(value->GetKey()) == 1);
        --ItemMapSize_;

        guard.Release();

        OnRemoved(value.Get());

        delete item;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie()
    : Active_(false)
{ }

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(const TKey& key)
    : Key_(key)
    , Active_(false)
{ }

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(TInsertCookie&& other)
    : Key_(std::move(other.Key))
    , Cache_(std::move(other.Cache))
    , ValueOrError_(std::move(other.ValueOrError_))
    , Active_(other.Active)
{
    other.Active = false;
}

template <class TKey, class TValue, class THash>
TCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    Abort();
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TInsertCookie& TCacheBase<TKey, TValue, THash>::TInsertCookie::operator =(TInsertCookie&& other)
{
    if (this != &other) {
        Abort();
        Key_ = std::move(other.Key_);
        Cache_ = std::move(other.Cache_);
        ValueOrError_ = std::move(other.ValueOrError_);
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

template <class TKey, class TValue, class THash>
TKey TCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
typename TCacheBase<TKey, TValue, THash>::TAsyncValuePtrOrErrorResult
TCacheBase<TKey, TValue, THash>::TInsertCookie::GetValue() const
{
    YASSERT(ValueOrError_);
    return ValueOrError_;
}

template <class TKey, class TValue, class THash>
bool TCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active_;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::Cancel(const TError& error)
{
    if (Active_) {
        Cache_->CancelInsert(Key_, error);
        Active_ = false;
    }
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    YCHECK(Active_);
    Cache_->EndInsert(value, this);
    Active_ = false;
}

template <class TKey, class TValue, class THash>
void TCacheBase<TKey, TValue, THash>::TInsertCookie::Abort()
{
    Cancel(TError("Cache item insertion aborted"));
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSizeLimitedCache<TKey, TValue, THash>::TSizeLimitedCache(int maxSize)
    : MaxSize_(maxSize)
{ }

template <class TKey, class TValue, class THash>
bool TSizeLimitedCache<TKey, TValue, THash>::IsTrimNeeded() const
{
    return this->GetSize() > MaxSize_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TWeightLimitedCache<TKey, TValue, THash>::TWeightLimitedCache(i64 maxWeight)
    : TotalWeight_(0)
    , MaxWeight_(maxWeight)
{ }

template <class TKey, class TValue, class THash>
void TWeightLimitedCache<TKey, TValue, THash>::OnAdded(TValue* value)
{
    TGuard<TSpinLock> guard(this->SpinLock_);
    TotalWeight_ += GetWeight(value);
}

template <class TKey, class TValue, class THash>
void TWeightLimitedCache<TKey, TValue, THash>::OnRemoved(TValue* value)
{
    TGuard<TSpinLock> guard(this->SpinLock_);
    TotalWeight_ -= GetWeight(value);
}

template <class TKey, class TValue, class THash>
bool TWeightLimitedCache<TKey, TValue, THash>::IsTrimNeeded() const
{
    return TotalWeight_ > MaxWeight_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
