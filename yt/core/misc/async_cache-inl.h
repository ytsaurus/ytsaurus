#ifndef ASYNC_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_cache.h"
#endif
#undef ASYNC_CACHE_INL_H_

#include "config.h"

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TAsyncCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
TAsyncCacheValueBase<TKey, TValue, THash>::TAsyncCacheValueBase(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TValue, class THash>
NYT::TAsyncCacheValueBase<TKey, TValue, THash>::~TAsyncCacheValueBase()
{
    if (Cache_) {
        Cache_->Unregister(Key_);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TAsyncSlruCacheBase(TSlruCacheConfigPtr config)
    : Config_(std::move(config))
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Clear()
{
    TIntrusiveListWithAutoDelete<TItem, TDelete> youngerLruList;
    TIntrusiveListWithAutoDelete<TItem, TDelete> olderLruList;

    {
        NConcurrency::TWriterGuard guard(SpinLock_);

        ItemMap_.clear();
        ItemMapSize_ = 0;

        YoungerLruList_.Swap(youngerLruList);
        YoungerWeight_ = 0;

        OlderLruList_.Swap(olderLruList);
        OlderWeight_ = 0;
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TAsyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    NConcurrency::TReaderGuard guard(SpinLock_);

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return nullptr;
    }

    auto* item = itemIt->second;
    bool canTouch = CanTouch(item);
    auto value = item->Value;

    guard.Release();

    if (canTouch) {
        Touch(key);
    }

    return value;
}

template <class TKey, class TValue, class THash>
int TAsyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return ItemMapSize_;
}

template <class TKey, class TValue, class THash>
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::GetAll()
{
    NConcurrency::TReaderGuard guard(SpinLock_);

    std::vector<TValuePtr> result;
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
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    while (true) {
        NConcurrency::TReaderGuard readerGuard(SpinLock_);

        auto itemIt = ItemMap_.find(key);
        if (itemIt != ItemMap_.end()) {
            auto* item = itemIt->second;
            bool canTouch = CanTouch(item);
            auto promise = item->ValuePromise;

            readerGuard.Release();

            if (canTouch) {
                Touch(key);
            }

            return promise;
        }

        auto valueIt = ValueMap_.find(key);
        if (valueIt == ValueMap_.end()) {
            return TValueFuture();
        }

        auto value = TRefCounted::DangerousGetPtr(valueIt->second);

        readerGuard.Release();

        if (value) {
            NConcurrency::TWriterGuard writerGuard(SpinLock_);

            if (ItemMap_.find(key) != ItemMap_.end())
                continue;

            auto* item = new TItem(value);
            // This holds an extra reference to the promise state...
            auto promise = item->ValuePromise;

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            PushToYounger(item);

            writerGuard.Release();

            // ...since the item can be dead at this moment.
            TrimIfNeeded();

            return promise;
        }

        // Back off.
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::BeginInsert(TInsertCookie* cookie)
{
    YCHECK(!cookie->Active_);
    auto key = cookie->GetKey();

    while (true) {
        NConcurrency::TWriterGuard guard(SpinLock_);

        auto itemIt = ItemMap_.find(key);
        if (itemIt != ItemMap_.end()) {
            auto* item = itemIt->second;
            cookie->ValuePromise_ = item->ValuePromise;
            return false;
        }

        auto valueIt = ValueMap_.find(key);
        if (valueIt == ValueMap_.end()) {
            auto* item = new TItem();

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            cookie->ValuePromise_ = item->ValuePromise;
            cookie->Active_ = true;
            cookie->Cache_ = this;

            return true;
        }

        auto value = TRefCounted::DangerousGetPtr(valueIt->second);
        if (value) {
            auto* item = new TItem(value);

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            PushToYounger(item);

            cookie->ValuePromise_ = item->ValuePromise;

            guard.Release();

            TrimIfNeeded();

            return false;
        }

        // Back off.
        // Hopefully the object we had just extracted will be destroyed soon
        // and thus vanish from ValueMap.
        guard.Release();
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::EndInsert(TValuePtr value, TInsertCookie* cookie)
{
    YCHECK(cookie->Active_);

    auto key = value->GetKey();

    NConcurrency::TWriterGuard guard(SpinLock_);

    YCHECK(!value->Cache_);
    value->Cache_ = this;

    auto it = ItemMap_.find(key);
    YCHECK(it != ItemMap_.end());

    auto* item = it->second;
    item->Value = value;
    auto promise = item->ValuePromise;

    YCHECK(ValueMap_.insert(std::make_pair(key, value.Get())).second);

    PushToYounger(item);

    guard.Release();

    promise.Set(value);

    OnAdded(value.Get());

    TrimIfNeeded();
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    TValuePromise promise;
    {
        NConcurrency::TWriterGuard guard(SpinLock_);

        auto it = ItemMap_.find(key);
        YCHECK(it != ItemMap_.end());

        auto* item = it->second;
        promise = item->ValuePromise;

        ItemMap_.erase(it);
        --ItemMapSize_;

        delete item;
    }

    promise.Set(error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    YCHECK(ItemMap_.find(key) == ItemMap_.end());
    YCHECK(ValueMap_.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return false;
    }

    auto* item = it->second;
    auto value = item->Value;

    ItemMap_.erase(it);
    --ItemMapSize_;

    Pop(item);

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TAsyncCacheValueBase::TAsyncCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    OnRemoved(value.Get());

    delete item;

    return true;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(TValuePtr value)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

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

        Pop(item);

        delete item;
    }

    YASSERT(value->Cache_);
    value->Cache_.Reset();

    guard.Release();

    OnRemoved(value.Get());

    return true;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::CanTouch(TItem* item)
{
    return NProfiling::GetCpuInstant() >= item->NextTouchInstant;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    static auto MinTouchPeriod = TDuration::MilliSeconds(100);

    NConcurrency::TWriterGuard guard(SpinLock_);

    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end())
        return;

    auto* item = it->second;
    if (!item->Empty()) {
        MoveToOlder(item);
    }

    item->NextTouchInstant = NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(MinTouchPeriod);
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::GetWeight(TValue* /*value*/) const
{
    return 1;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnAdded(TValue* /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(TValue* /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::PushToYounger(TItem* item)
{
    YASSERT(item->Empty());
    YoungerLruList_.PushFront(item);
    YoungerWeight_ += GetWeight(item->Value.Get());
    item->Younger = true;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TItem* item)
{
    YASSERT(!item->Empty());
    item->Unlink();
    YoungerLruList_.PushFront(item);
    if (!item->Younger) {
        i64 weight = GetWeight(item->Value.Get());
        OlderWeight_ -= weight;
        YoungerWeight_ += weight;
        item->Younger = true;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TItem* item)
{
    YASSERT(!item->Empty());
    item->Unlink();
    OlderLruList_.PushFront(item);
    if (item->Younger) {
        i64 weight = GetWeight(item->Value.Get());
        YoungerWeight_ -= weight;
        OlderWeight_ += weight;
        item->Younger = false;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Pop(TItem* item)
{
    if (item->Empty())
        return;
    i64 weight = GetWeight(item->Value.Get());
    if (item->Younger) {
        YoungerWeight_ -= weight;
    } else {
        OlderWeight_ -= weight;
    }
    item->Unlink();
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TrimIfNeeded()
{
    // Move from older to younger.
    while (true) {
        NConcurrency::TWriterGuard guard(SpinLock_);

        if (OlderLruList_.Empty() || OlderWeight_ <= Config_->Capacity * (1 - Config_->YoungerSizeFraction))
            break;

        auto* item = &*(--OlderLruList_.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    while (true) {
        NConcurrency::TWriterGuard guard(SpinLock_);

        if (YoungerLruList_.Empty() || YoungerWeight_ + OlderWeight_ <= Config_->Capacity)
            break;

        auto* item = &*(--YoungerLruList_.End());
        auto value = item->Value;

        Pop(item);

        YCHECK(ItemMap_.erase(value->GetKey()) == 1);
        --ItemMapSize_;

        guard.Release();

        OnRemoved(value.Get());

        delete item;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie()
    : Active_(false)
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(const TKey& key)
    : Key_(key)
      , Active_(false)
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(TInsertCookie&& other)
    : Key_(std::move(other.Key_))
      , Cache_(std::move(other.Cache_))
      , ValuePromise_(std::move(other.ValuePromise_))
      , Active_(other.Active_)
{
    other.Active_ = false;
}

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    Abort();
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie& TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::operator =(TInsertCookie&& other)
{
    if (this != &other) {
        Abort();
        Key_ = std::move(other.Key_);
        Cache_ = std::move(other.Cache_);
        ValuePromise_ = std::move(other.ValuePromise_);
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

template <class TKey, class TValue, class THash>
const TKey& TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::GetValue() const
{
    YASSERT(ValuePromise_);
    return ValuePromise_;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active_;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::Cancel(const TError& error)
{
    if (Active_) {
        Cache_->CancelInsert(Key_, error);
        Active_ = false;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    YCHECK(Active_);
    Cache_->EndInsert(value, this);
    Active_ = false;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::Abort()
{
    Cancel(TError("Cache item insertion aborted"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
