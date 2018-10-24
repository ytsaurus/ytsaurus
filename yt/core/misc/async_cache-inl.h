#pragma once
#ifndef ASYNC_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_cache.h"
// For the sake of sane code completion
#include "async_cache.h"
#endif

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
    auto cache = Cache_.Lock();
    if (cache) {
        cache->Unregister(Key_);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , TouchBuffer_(Config_->TouchBufferCapacity)
    , Profiler(profiler)
    , HitWeightCounter_("/hit")
    , MissedWeightCounter_("/missed")
    , YoungerWeightCounter_("/younger")
    , OlderWeightCounter_("/older")
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Clear()
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    TouchBufferPosition_ = 0;

    ItemMap_.clear();
    ItemMapSize_ = 0;

    TIntrusiveListWithAutoDelete<TItem, TDelete> youngerLruList;
    YoungerLruList_.Swap(youngerLruList);
    Profiler.Update(YoungerWeightCounter_, 0);

    TIntrusiveListWithAutoDelete<TItem, TDelete> olderLruList;
    OlderLruList_.Swap(olderLruList);
    Profiler.Update(OlderWeightCounter_, 0);

    // NB: Lists must die outside the critical section.
    guard.Release();
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TAsyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    NConcurrency::TReaderGuard readerGuard(SpinLock_);

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return nullptr;
    }

    auto* item = itemIt->second;
    auto value = item->Value;
    if (!value) {
        return nullptr;
    }

    bool needToDrain = Touch(item);

    auto weight = GetWeight(item->Value);
    Profiler.Increment(HitWeightCounter_, weight);

    readerGuard.Release();

    if (needToDrain) {
        NConcurrency::TWriterGuard writerGuard(SpinLock_);
        DrainTouchBuffer();
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
            bool needToDrain = Touch(item);
            auto promise = item->ValuePromise;

            if (item->Value) {
                auto weight = GetWeight(item->Value);
                Profiler.Increment(HitWeightCounter_, weight);
            }

            readerGuard.Release();

            if (needToDrain) {
                NConcurrency::TWriterGuard guard(SpinLock_);
                DrainTouchBuffer();
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

            DrainTouchBuffer();

            if (ItemMap_.find(key) != ItemMap_.end())
                continue;

            auto* item = new TItem(value);
            // This holds an extra reference to the promise state...
            auto promise = item->ValuePromise;

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            auto weight = PushToYounger(item);
            Profiler.Increment(HitWeightCounter_, weight);

            Trim(writerGuard);
            // ...since the item can be dead at this moment.

            return promise;
        }

        // Back off.
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::BeginInsert(const TKey& key) -> TInsertCookie
{
    while (true) {
        NConcurrency::TWriterGuard guard(SpinLock_);

        DrainTouchBuffer();

        auto itemIt = ItemMap_.find(key);
        if (itemIt != ItemMap_.end()) {
            auto* item = itemIt->second;
            auto valuePromise = item->ValuePromise;

            if (item->Value) {
                auto weight = GetWeight(item->Value);
                Profiler.Increment(HitWeightCounter_, weight);
            }

            return TInsertCookie(
                key,
                nullptr,
                std::move(valuePromise),
                false);
        }

        auto valueIt = ValueMap_.find(key);
        if (valueIt == ValueMap_.end()) {
            auto* item = new TItem();
            auto valuePromise = item->ValuePromise;

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            return TInsertCookie(
                key,
                this,
                std::move(valuePromise),
                true);
        }

        auto value = TRefCounted::DangerousGetPtr(valueIt->second);
        if (value) {
            auto* item = new TItem(value);
            auto value = item->Value;
            Y_ASSERT(value);

            YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
            ++ItemMapSize_;

            auto weight = PushToYounger(item);
            Profiler.Increment(HitWeightCounter_, weight);

            // NB: Releases the lock.
            Trim(guard);

            OnAdded(value);

            return TInsertCookie(
                key,
                nullptr,
                MakeFuture(value),
                false);
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
    YCHECK(value);

    auto key = value->GetKey();

    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    value->Cache_ = MakeWeak(this);

    auto it = ItemMap_.find(key);
    YCHECK(it != ItemMap_.end());

    auto* item = it->second;
    item->Value = value;
    auto promise = item->ValuePromise;

    YCHECK(ValueMap_.insert(std::make_pair(key, value.Get())).second);

    auto weight = PushToYounger(item);
    Profiler.Increment(MissedWeightCounter_, weight);

    // NB: Releases the lock.
    Trim(guard);

    promise.Set(value);

    OnAdded(value);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    auto it = ItemMap_.find(key);
    YCHECK(it != ItemMap_.end());

    auto* item = it->second;
    auto promise = item->ValuePromise;

    ItemMap_.erase(it);
    --ItemMapSize_;

    delete item;

    guard.Release();

    promise.Set(error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    YCHECK(ItemMap_.find(key) == ItemMap_.end());
    YCHECK(ValueMap_.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return false;
    }

    auto* item = it->second;
    auto value = item->Value;
    if (!value) {
        return false;
    }

    ItemMap_.erase(it);
    --ItemMapSize_;

    Pop(item);

    // Release the guard right away to prevent recursive spinlock acquisition.
    // Indeed, the item's dtor may drop the last reference
    // to the value and thus cause an invocation of TAsyncCacheValueBase::TAsyncCacheValueBase.
    // The latter will try to acquire the spinlock.
    guard.Release();

    OnRemoved(value);

    delete item;

    return true;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(TValuePtr value)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

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

    value->Cache_.Reset();

    guard.Release();

    OnRemoved(value);

    return true;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::Touch(TItem* item)
{
    int capacity = TouchBuffer_.size();
    int index = TouchBufferPosition_++;
    if (index >= capacity) {
        // Drop touch request due to buffer overflow.
        // NB: We still return false since the other thread is already responsible for
        // draining the buffer.
        return false;
    }

    TouchBuffer_[index] = item;
    return index == capacity - 1;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::DrainTouchBuffer()
{
    int count = std::min(
        TouchBufferPosition_.load(),
        static_cast<int>(TouchBuffer_.size()));
    for (int index = 0; index < count; ++index) {
        MoveToOlder(TouchBuffer_[index]);
    }
    TouchBufferPosition_ = 0;
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::GetWeight(const TValuePtr& /*value*/) const
{
    return 1;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::PushToYounger(TItem* item)
{
    Y_ASSERT(item->Empty());
    YoungerLruList_.PushFront(item);
    auto weight = GetWeight(item->Value);
    Profiler.Increment(YoungerWeightCounter_, +weight);
    item->Younger = true;
    return weight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TItem* item)
{
    Y_ASSERT(!item->Empty());
    item->Unlink();
    YoungerLruList_.PushFront(item);
    if (!item->Younger) {
        auto weight = GetWeight(item->Value);
        Profiler.Increment(OlderWeightCounter_, -weight);
        Profiler.Increment(YoungerWeightCounter_, +weight);
        item->Younger = true;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TItem* item)
{
    Y_ASSERT(!item->Empty());
    item->Unlink();
    OlderLruList_.PushFront(item);
    if (item->Younger) {
        auto weight = GetWeight(item->Value);
        Profiler.Increment(YoungerWeightCounter_, -weight);
        Profiler.Increment(OlderWeightCounter_, +weight);
        item->Younger = false;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Pop(TItem* item)
{
    if (item->Empty())
        return;
    auto weight = GetWeight(item->Value);
    if (item->Younger) {
        Profiler.Increment(YoungerWeightCounter_, -weight);
    } else {
        Profiler.Increment(OlderWeightCounter_, -weight);
    }
    item->Unlink();
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Trim(NConcurrency::TWriterGuard& guard)
{
    // Move from older to younger.
    while (!OlderLruList_.Empty() &&
           OlderWeightCounter_.GetCurrent() > Config_->Capacity * (1 - Config_->YoungerSizeFraction))
    {
        auto* item = &*(--OlderLruList_.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    std::vector<TValuePtr> evictedValues;
    while (!YoungerLruList_.Empty() &&
           YoungerWeightCounter_.GetCurrent() + OlderWeightCounter_.GetCurrent() > Config_->Capacity)
    {
        auto* item = &*(--YoungerLruList_.End());
        auto value = item->Value;

        Pop(item);

        YCHECK(ItemMap_.erase(value->GetKey()) == 1);
        --ItemMapSize_;

        evictedValues.emplace_back(std::move(item->Value));

        delete item;
    }

    guard.Release();

    for (const auto& value : evictedValues) {
        OnRemoved(value);
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
    , ValueFuture_(std::move(other.ValueFuture_))
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
        ValueFuture_ = std::move(other.ValueFuture_);
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
    Y_ASSERT(ValueFuture_);
    return ValueFuture_;
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
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(
    const TKey& key,
    TIntrusivePtr<TAsyncSlruCacheBase> cache,
    TValueFuture valueFuture,
    bool active)
    : Key_(key)
    , Cache_(std::move(cache))
    , ValueFuture_(std::move(valueFuture))
    , Active_(active)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::Abort()
{
    Cancel(TError("Cache item insertion aborted"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
