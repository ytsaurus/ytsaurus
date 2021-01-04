#pragma once
#ifndef ASYNC_SLRU_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_slru_cache.h"
// For the sake of sane code completion.
#include "async_slru_cache.h"
#endif

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
    const NProfiling::TRegistry& profiler)
    : Config_(std::move(config))
    , Capacity_(Config_->Capacity)
    , YoungerSizeFraction_(Config_->YoungerSizeFraction)
    , TouchBuffer_(Config_->TouchBufferCapacity)
    , HitWeightCounter_(profiler.Counter("/hit"))
    , MissedWeightCounter_(profiler.Counter("/missed"))
{
    profiler.AddFuncGauge("/younger", MakeStrong(this), [this] {
        return YoungerWeightCounter_.load();
    });
    profiler.AddFuncGauge("/older", MakeStrong(this), [this] {
        return OlderWeightCounter_.load();
    });
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Clear()
{
    auto guard = WriterGuard(SpinLock_);

    TouchBufferPosition_ = 0;

    ItemMap_.clear();
    ItemMapSize_ = 0;

    TIntrusiveListWithAutoDelete<TItem, TDelete> youngerLruList;
    YoungerLruList_.Swap(youngerLruList);
    YoungerWeightCounter_ = 0;

    TIntrusiveListWithAutoDelete<TItem, TDelete> olderLruList;
    OlderLruList_.Swap(olderLruList);
    OlderWeightCounter_ = 0;

    // NB: Lists must die outside the critical section.
    guard.Release();
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    Capacity_.store(config->Capacity.value_or(Config_->Capacity));
    YoungerSizeFraction_.store(config->YoungerSizeFraction.value_or(Config_->YoungerSizeFraction));

    {
        auto writerGuard = WriterGuard(SpinLock_);

        DrainTouchBuffer();
        Trim(writerGuard);
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TAsyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    auto readerGuard = ReaderGuard(SpinLock_);

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
    HitWeightCounter_.Increment(weight);

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(SpinLock_);
        DrainTouchBuffer();
    }

    return value;
}

template <class TKey, class TValue, class THash>
int TAsyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return ItemMapSize_.load();
}

template <class TKey, class TValue, class THash>
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::GetAll()
{
    auto guard = ReaderGuard(SpinLock_);

    std::vector<TValuePtr> result;
    result.reserve(ValueMap_.size());
    for (const auto& [key, rawValue] : ValueMap_) {
        if (auto value = DangerousGetPtr<TValue>(rawValue)) {
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
        auto readerGuard = ReaderGuard(SpinLock_);

        auto itemIt = ItemMap_.find(key);
        if (itemIt != ItemMap_.end()) {
            auto* item = itemIt->second;
            bool needToDrain = Touch(item);
            auto promise = item->ValuePromise;

            if (item->Value) {
                auto weight = GetWeight(item->Value);
                HitWeightCounter_.Increment(weight);
            }

            readerGuard.Release();

            if (needToDrain) {
                auto guard = WriterGuard(SpinLock_);
                DrainTouchBuffer();
            }

            return promise;
        }

        auto valueIt = ValueMap_.find(key);
        if (valueIt == ValueMap_.end()) {
            return TValueFuture();
        }

        auto value = DangerousGetPtr(valueIt->second);

        readerGuard.Release();

        if (value) {
            auto writerGuard = WriterGuard(SpinLock_);

            DrainTouchBuffer();

            if (ItemMap_.find(key) != ItemMap_.end())
                continue;

            auto* item = new TItem(value);
            // This holds an extra reference to the promise state...
            auto promise = item->ValuePromise;

            YT_VERIFY(ItemMap_.emplace(key, item).second);
            ++ItemMapSize_;

            auto weight = PushToYounger(item);
            HitWeightCounter_.Increment(weight);

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
        auto guard = WriterGuard(SpinLock_);

        DrainTouchBuffer();

        auto itemIt = ItemMap_.find(key);
        if (itemIt != ItemMap_.end()) {
            auto* item = itemIt->second;
            auto valueFuture = item->ValuePromise
                .ToFuture()
                .ToUncancelable();

            if (item->Value) {
                auto weight = GetWeight(item->Value);
                HitWeightCounter_.Increment(weight);
            }

            return TInsertCookie(
                key,
                nullptr,
                std::move(valueFuture),
                false);
        }

        auto valueIt = ValueMap_.find(key);
        if (valueIt == ValueMap_.end()) {
            auto* item = new TItem();
            auto valuePromise = item->ValuePromise
                .ToFuture()
                .ToUncancelable();

            YT_VERIFY(ItemMap_.emplace(key, item).second);
            ++ItemMapSize_;

            return TInsertCookie(
                key,
                this,
                std::move(valuePromise),
                true);
        }

        auto value = DangerousGetPtr(valueIt->second);
        if (value) {
            auto* item = new TItem(value);

            YT_VERIFY(ItemMap_.emplace(key, item).second);
            ++ItemMapSize_;

            auto weight = PushToYounger(item);
            HitWeightCounter_.Increment(weight);

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
void TAsyncSlruCacheBase<TKey, TValue, THash>::EndInsert(TValuePtr value)
{
    YT_VERIFY(value);
    auto key = value->GetKey();

    auto guard = WriterGuard(SpinLock_);

    DrainTouchBuffer();

    value->Cache_ = MakeWeak(this);

    auto* item = GetOrCrash(ItemMap_, key);
    item->Value = value;
    auto promise = item->ValuePromise;

    YT_VERIFY(ValueMap_.emplace(key, value.Get()).second);

    auto weight = PushToYounger(item);
    MissedWeightCounter_.Increment(weight);

    // NB: Releases the lock.
    Trim(guard);

    promise.Set(value);

    OnAdded(value);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    auto guard = WriterGuard(SpinLock_);

    DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    YT_VERIFY(itemIt != ItemMap_.end());

    auto* item = itemIt->second;
    auto promise = item->ValuePromise;

    ItemMap_.erase(itemIt);
    --ItemMapSize_;

    delete item;

    guard.Release();

    promise.Set(error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    auto guard = WriterGuard(SpinLock_);

    DrainTouchBuffer();

    YT_VERIFY(ItemMap_.find(key) == ItemMap_.end());
    YT_VERIFY(ValueMap_.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key, bool forbidResurrection)
{
    DoTryRemove(key, nullptr, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TValuePtr& value, bool forbidResurrection)
{
    DoTryRemove(value->GetKey(), value, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::DoTryRemove(
    const TKey& key,
    const TValuePtr& value,
    bool forbidResurrection)
{
    auto guard = WriterGuard(SpinLock_);

    DrainTouchBuffer();

    auto valueIt = ValueMap_.find(key);
    if (valueIt == ValueMap_.end()) {
        return;
    }

    if (value && valueIt->second != value) {
        return;
    }

    if (forbidResurrection || !IsResurrectionSupported()) {
        valueIt->second->Cache_.Reset();
        ValueMap_.erase(valueIt);
    }

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return;
    }

    auto* item = itemIt->second;
    auto actualValue = item->Value;
    if (!actualValue) {
        return;
    }

    ItemMap_.erase(itemIt);
    --ItemMapSize_;

    Pop(item);

    delete item;

    guard.Release();

    OnRemoved(actualValue);
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
bool TAsyncSlruCacheBase<TKey, TValue, THash>::IsResurrectionSupported() const
{
    return true;
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::PushToYounger(TItem* item)
{
    YT_ASSERT(item->Empty());
    YoungerLruList_.PushFront(item);
    auto weight = GetWeight(item->Value);
    YoungerWeightCounter_.fetch_add(weight);
    item->Younger = true;
    return weight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    YoungerLruList_.PushFront(item);
    if (!item->Younger) {
        auto weight = GetWeight(item->Value);
        OlderWeightCounter_.fetch_sub(weight);
        YoungerWeightCounter_.fetch_add(weight);
        item->Younger = true;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    OlderLruList_.PushFront(item);
    if (item->Younger) {
        auto weight = GetWeight(item->Value);
        YoungerWeightCounter_.fetch_sub(weight);
        OlderWeightCounter_.fetch_add(weight);
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
        YoungerWeightCounter_.fetch_sub(weight);
    } else {
        OlderWeightCounter_.fetch_sub(weight);
    }
    item->Unlink();
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Trim(NConcurrency::TSpinlockWriterGuard<NConcurrency::TReaderWriterSpinLock>& guard)
{
    // Move from older to younger.
    auto capacity = Capacity_.load();
    auto youngerSizeFraction = YoungerSizeFraction_.load();
    while (!OlderLruList_.Empty() &&
           OlderWeightCounter_.load() > capacity * (1 - youngerSizeFraction))
    {
        auto* item = &*(--OlderLruList_.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    std::vector<TValuePtr> evictedValues;
    while (!YoungerLruList_.Empty() &&
           YoungerWeightCounter_.load() + OlderWeightCounter_.load() > capacity)
    {
        auto* item = &*(--YoungerLruList_.End());
        auto value = item->Value;

        Pop(item);

        YT_VERIFY(ItemMap_.erase(value->GetKey()) == 1);
        --ItemMapSize_;

        if (!IsResurrectionSupported()) {
            YT_VERIFY(ValueMap_.erase(value->GetKey()) == 1);
            value->Cache_.Reset();
        }

        evictedValues.push_back(std::move(value));

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
    , Active_(other.Active_.exchange(false))
{ }

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
        Active_ = other.Active_.exchange(false);
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
    YT_ASSERT(ValueFuture_);
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
    auto expected = true;
    if (!Active_.compare_exchange_strong(expected, false)) {
        return;
    }

    Cache_->CancelInsert(Key_, error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    auto expected = true;
    if (!Active_.compare_exchange_strong(expected, false)) {
        return;
    }

    Cache_->EndInsert(value);
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

template <class TKey, class TValue, class THash>
TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::TMemoryTrackingAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TRegistry& profiler)
    : TAsyncSlruCacheBase<TKey, TValue, THash>(
        std::move(config),
        profiler)
    , MemoryTrackerGuard_(TMemoryUsageTrackerGuard::Acquire(
        std::move(memoryTracker),
        this->Config_->Capacity))
{ }

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    MemoryTrackerGuard_.SetSize(config->Capacity.value_or(this->Config_->Capacity));
    TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
