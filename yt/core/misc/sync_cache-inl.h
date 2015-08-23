#ifndef SYNC_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_cache.h"
#endif

#include "config.h"

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TSyncCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
TSyncCacheValueBase<TKey, TValue, THash>::TSyncCacheValueBase(const TKey& key)
    : Key_(key)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSyncSlruCacheBase<TKey, TValue, THash>::TSyncSlruCacheBase(
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
void TSyncSlruCacheBase<TKey, TValue, THash>::Clear()
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

    Profiler.Update(HitWeightCounter_, 0);
    Profiler.Update(MissedWeightCounter_, 0);

    guard.Release();

    // NB: Lists must die outside the critical section.
}

template <class TKey, class TValue, class THash>
typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TSyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    NConcurrency::TReaderGuard readerGuard(SpinLock_);

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return nullptr;
    }

    auto* item = itemIt->second;
    bool needToDrain = Touch(item);
    auto value = item->Value;

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
std::vector<typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TSyncSlruCacheBase<TKey, TValue, THash>::GetAll()
{
    NConcurrency::TReaderGuard guard(SpinLock_);

    std::vector<TValuePtr> result;
    result.reserve(ItemMap_.size());
    for (const auto& pair : ItemMap_) {
        result.push_back(pair.second->Value);
    }
    return result;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryInsert(TValuePtr value, TValuePtr* existingValue)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    const auto& key = value->GetKey();

    auto itemIt = ItemMap_.find(key);
    if (itemIt != ItemMap_.end()) {
        if (existingValue) {
            *existingValue = itemIt->second->Value;
        }
        return false;
    }

    auto* item = new TItem(value);
    YCHECK(ItemMap_.insert(std::make_pair(key, item)).second);
    ++ItemMapSize_;

    auto weight = GetWeight(value);
    Profiler.Increment(MissedWeightCounter_, weight);

    PushToYounger(item);

    // NB: Releases the lock.
    Trim(guard);

    OnAdded(value);

    return true;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Trim(NConcurrency::TWriterGuard& guard)
{
    // Move from older to younger.
    while (!OlderLruList_.Empty() &&
           OlderWeightCounter_.Current > Config_->Capacity * (1 - Config_->YoungerSizeFraction))
    {
        auto* item = &*(--OlderLruList_.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    std::vector<TValuePtr> evictedValues;
    while (!YoungerLruList_.Empty() &&
           YoungerWeightCounter_.Current + OlderWeightCounter_.Current > Config_->Capacity)
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

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return false;
    }

    auto* item = it->second;
    auto value = item->Value;

    ItemMap_.erase(it);
    --ItemMapSize_;

    Pop(item);

    delete item;

    guard.Release();

    OnRemoved(value);

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryRemove(TValuePtr value)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    DrainTouchBuffer();

    auto itemIt = ItemMap_.find(value->GetKey());
    if (itemIt == ItemMap_.end()) {
        return false;
    }

    auto* item = itemIt->second;
    if (item->Value != value) {
        return false;
    }

    ItemMap_.erase(itemIt);
    --ItemMapSize_;

    Pop(item);

    delete item;

    guard.Release();

    OnRemoved(value);

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::Touch(TItem* item)
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
void TSyncSlruCacheBase<TKey, TValue, THash>::DrainTouchBuffer()
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
i64 TSyncSlruCacheBase<TKey, TValue, THash>::GetWeight(const TValuePtr& /*value*/) const
{
    return 1;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
int TSyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return ItemMapSize_;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::PushToYounger(TItem* item)
{
    YASSERT(item->Empty());
    YoungerLruList_.PushFront(item);
    auto weight = GetWeight(item->Value);
    Profiler.Increment(YoungerWeightCounter_, +weight);
    item->Younger = true;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TItem* item)
{
    YASSERT(!item->Empty());
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
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TItem* item)
{
    YASSERT(!item->Empty());
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
void TSyncSlruCacheBase<TKey, TValue, THash>::Pop(TItem* item)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
