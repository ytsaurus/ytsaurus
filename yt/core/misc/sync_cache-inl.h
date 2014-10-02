#ifndef SYNC_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_cache.h"
#endif
#undef SYNC_CACHE_INL_H_

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
TSyncSlruCacheBase<TKey, TValue, THash>::TSyncSlruCacheBase(TSlruCacheConfigPtr config)
    : Config_(std::move(config))
{ }

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Clear()
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
typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TSyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
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

    PushToYounger(item);

    guard.Release();

    OnAdded(value.Get());

    TrimIfNeeded();

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::Remove(const TKey& key)
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

    guard.Release();

    OnRemoved(value.Get());

    delete item;

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::Remove(TValuePtr value)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

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

    OnRemoved(value.Get());

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::CanTouch(TItem* item)
{
    return NProfiling::GetCpuInstant() >= item->NextTouchInstant;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Touch(const TKey& key)
{
    static auto MinTouchPeriod = TDuration::MilliSeconds(100);

    NConcurrency::TWriterGuard guard(SpinLock_);

    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end())
        return;

    auto* item = it->second;

    MoveToOlder(item);

    item->NextTouchInstant = NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(MinTouchPeriod);
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnAdded(TValue* /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(TValue* /*value*/)
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
    YoungerWeight_ += GetWeight(item->Value.Get());
    item->Younger = true;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TItem* item)
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
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TItem* item)
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
void TSyncSlruCacheBase<TKey, TValue, THash>::Pop(TItem* item)
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
void TSyncSlruCacheBase<TKey, TValue, THash>::TrimIfNeeded()
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

} // namespace NYT
