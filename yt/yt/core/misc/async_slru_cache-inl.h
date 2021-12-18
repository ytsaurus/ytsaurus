#ifndef ASYNC_SLRU_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_slru_cache.h"
// For the sake of sane code completion.
#include "async_slru_cache.h"
#endif

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::TItem()
    : ValuePromise(NewPromise<TValuePtr>())
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::TItem(TValuePtr value)
    : ValuePromise(MakePromise(value))
    , Value(std::move(value))
{ }

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::GetValueFuture() const -> TValueFuture
{
    return ValuePromise
        .ToFuture()
        .ToUncancelable();
}

////////////////////////////////////////////////////////////////////////////////

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::PushToYounger(TItem* item, i64 weight)
{
    YT_ASSERT(item->Empty());
    YoungerLruList.PushFront(item);
    item->CachedWeight = weight;
    YoungerWeightCounter += weight;
    AsDerived()->OnYoungerUpdated(1, weight);
    item->Younger = true;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::MoveToYounger(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    YoungerLruList.PushFront(item);
    if (!item->Younger) {
        i64 weight = item->CachedWeight;
        OlderWeightCounter -= weight;
        AsDerived()->OnOlderUpdated(-1, -weight);
        YoungerWeightCounter += weight;
        AsDerived()->OnYoungerUpdated(1, weight);
        item->Younger = true;
    }
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::MoveToOlder(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    OlderLruList.PushFront(item);
    if (item->Younger) {
        i64 weight = item->CachedWeight;
        YoungerWeightCounter -= weight;
        AsDerived()->OnYoungerUpdated(-1, -weight);
        OlderWeightCounter += weight;
        AsDerived()->OnOlderUpdated(1, weight);
        item->Younger = false;
    }
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::PopFromLists(TItem* item)
{
    if (item->Empty()) {
        return;
    }

    YT_VERIFY(TouchBufferPosition.load() == 0);

    i64 weight = item->CachedWeight;
    if (item->Younger) {
        YoungerWeightCounter -= weight;
        AsDerived()->OnYoungerUpdated(-1, -weight);
    } else {
        OlderWeightCounter -= weight;
        AsDerived()->OnOlderUpdated(-1, -weight);
    }
    item->Unlink();
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::UpdateWeight(TItem* item, i64 weightDelta)
{
    YT_VERIFY(!item->Empty());
    if (item->Younger) {
        YoungerWeightCounter += weightDelta;
        AsDerived()->OnYoungerUpdated(0, weightDelta);
    } else {
        OlderWeightCounter += weightDelta;
        AsDerived()->OnOlderUpdated(0, weightDelta);
    }
    item->CachedWeight += weightDelta;
}

template <class TItem, class TDerived>
TIntrusiveListWithAutoDelete<TItem, TDelete> TAsyncSlruCacheListManager<TItem, TDerived>::TrimNoDelete()
{
    // Move from older to younger.
    auto capacity = Capacity.load();
    auto youngerSizeFraction = YoungerSizeFraction.load();
    while (!OlderLruList.Empty() && OlderWeightCounter > capacity * (1 - youngerSizeFraction)) {
        auto* item = &*(--OlderLruList.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    TIntrusiveListWithAutoDelete<TItem, TDelete> evictedItems;
    while (!YoungerLruList.Empty() && static_cast<i64>(YoungerWeightCounter + OlderWeightCounter) > capacity) {
        auto* item = &*(--YoungerLruList.End());
        PopFromLists(item);
        evictedItems.PushBack(item);
    }

    return evictedItems;
}

template <class TItem, class TDerived>
bool TAsyncSlruCacheListManager<TItem, TDerived>::Touch(TItem* item)
{
    if (item->Empty()) {
        return false;
    }

    int capacity = std::ssize(TouchBuffer);
    int index = TouchBufferPosition++;
    if (index >= capacity) {
        // Drop touch request due to buffer overflow.
        // NB: We still return false since the other thread is already responsible for
        // draining the buffer.
        return false;
    }

    TouchBuffer[index] = item;
    return index == capacity - 1;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::DrainTouchBuffer()
{
    int count = std::min<int>(TouchBufferPosition.load(), std::ssize(TouchBuffer));
    for (int index = 0; index < count; ++index) {
        MoveToOlder(TouchBuffer[index]);
    }
    TouchBufferPosition = 0;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::Reconfigure(i64 capacity, double youngerSizeFraction)
{
    Capacity = capacity;
    YoungerSizeFraction = youngerSizeFraction;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::SetTouchBufferCapacity(i64 touchBufferCapacity)
{
    TouchBuffer.resize(touchBufferCapacity);
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::OnYoungerUpdated(i64 /*deltaCount*/, i64 /*deltaWeight*/)
{ }

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::OnOlderUpdated(i64 /*deltaCount*/, i64 /*deltaWeight*/)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TAsyncCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
void TAsyncCacheValueBase<TKey, TValue, THash>::UpdateWeight() const
{
    if (auto cache = Cache_.Lock()) {
        cache->UpdateWeight(GetKey());
    }
}

template <class TKey, class TValue, class THash>
TAsyncCacheValueBase<TKey, TValue, THash>::TAsyncCacheValueBase(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TValue, class THash>
NYT::TAsyncCacheValueBase<TKey, TValue, THash>::~TAsyncCacheValueBase()
{
    if (auto cache = Cache_.Lock()) {
        cache->Unregister(Key_);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Capacity_(Config_->Capacity)
    , SyncHitWeightCounter_(profiler.Counter("/hit_weight_sync"))
    , AsyncHitWeightCounter_(profiler.Counter("/hit_weight_async"))
    , MissedWeightCounter_(profiler.Counter("/missed_weight"))
    , SyncHitCounter_(profiler.Counter("/hit_count_sync"))
    , AsyncHitCounter_(profiler.Counter("/hit_count_async"))
    , MissedCounter_(profiler.Counter("/missed_count"))
{
    static_assert(
        std::is_base_of_v<TAsyncCacheValueBase<TKey, TValue, THash>, TValue>,
        "TValue must be derived from TAsyncCacheValueBase");

    profiler.AddFuncGauge("/younger_weight", MakeStrong(this), [this] {
        return YoungerWeightCounter_.load();
    });
    profiler.AddFuncGauge("/older_weight", MakeStrong(this), [this] {
        return OlderWeightCounter_.load();
    });
    profiler.AddFuncGauge("/younger_size", MakeStrong(this), [this] {
        return YoungerSizeCounter_.load();
    });
    profiler.AddFuncGauge("/older_size", MakeStrong(this), [this] {
        return OlderSizeCounter_.load();
    });

    YT_VERIFY(IsPowerOf2(Config_->ShardCount));
    Shards_.reset(new TShard[Config_->ShardCount]);

    i64 shardCapacity = std::max<i64>(1, Config_->Capacity / Config_->ShardCount);
    i64 touchBufferCapacity = Config_->TouchBufferCapacity / Config_->ShardCount;
    for (int index = 0; index < Config_->ShardCount; ++index) {
        Shards_[index].SetTouchBufferCapacity(touchBufferCapacity);
        Shards_[index].Reconfigure(shardCapacity, Config_->YoungerSizeFraction);
        Shards_[index].Parent = this;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    i64 capacity = config->Capacity.value_or(Config_->Capacity);
    i64 shardCapacity = std::max<i64>(1, Config_->Capacity / Config_->ShardCount);
    double youngerSizeFraction = config->YoungerSizeFraction.value_or(Config_->YoungerSizeFraction);
    Capacity_.store(capacity);

    for (int shardIndex = 0; shardIndex < Config_->ShardCount; ++shardIndex) {
        auto& shard = Shards_[shardIndex];

        auto writerGuard = WriterGuard(shard.SpinLock);
        shard.Reconfigure(shardCapacity, youngerSizeFraction);
        shard.DrainTouchBuffer();
        NotifyOnTrim(shard.Trim(writerGuard), nullptr);
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TAsyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto readerGuard = ReaderGuard(shard->SpinLock);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        MissedCounter_.Increment();
        return nullptr;
    }

    auto* item = itemIt->second;
    auto value = item->Value;
    if (!value) {
        MissedCounter_.Increment();
        return nullptr;
    }

    bool needToDrain = shard->Touch(item);

    SyncHitWeightCounter_.Increment(item->CachedWeight);
    SyncHitCounter_.Increment();

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(shard->SpinLock);
        shard->DrainTouchBuffer();
    }

    return value;
}

template <class TKey, class TValue, class THash>
int TAsyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return Size_.load();
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::GetCapacity() const
{
    return Capacity_.load();
}

template <class TKey, class TValue, class THash>
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::GetAll()
{
    std::vector<TValuePtr> result;
    result.reserve(GetSize());

    for (int shardIndex = 0; shardIndex < Config_->ShardCount; ++shardIndex) {
        const auto& shard = Shards_[shardIndex];

        auto readerGuard = ReaderGuard(shard.SpinLock);
        for (const auto& [key, rawValue] : shard.ValueMap) {
            if (auto value = DangerousGetPtr<TValue>(rawValue)) {
                result.push_back(value);
            }
        }
    }
    return result;
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    auto* shard = GetShardByKey(key);
    auto valueFuture = DoLookup(shard, key);
    if (!valueFuture) {
        MissedCounter_.Increment();
    }
    return valueFuture;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Touch(const TValuePtr& value)
{
    auto* shard = GetShardByKey(value->GetKey());
    auto readerGuard = ReaderGuard(shard->SpinLock);

    if (value->Cache_.Lock() != this || !value->Item_) {
        return;
    }

    auto needToDrain = shard->Touch(value->Item_);

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(shard->SpinLock);
        shard->DrainTouchBuffer();
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::DoLookup(TShard* shard, const TKey& key)
{
    auto readerGuard = ReaderGuard(shard->SpinLock);

    auto& itemMap = shard->ItemMap;
    const auto& valueMap = shard->ValueMap;

    if (auto itemIt = itemMap.find(key); itemIt != itemMap.end()) {
        auto* item = itemIt->second;
        bool needToDrain = shard->Touch(item);
        auto valueFuture = item->GetValueFuture();

        if (item->Value) {
            SyncHitWeightCounter_.Increment(item->CachedWeight);
            SyncHitCounter_.Increment();
        } else {
            AsyncHitCounter_.Increment();
            item->AsyncHitCount.fetch_add(1);
        }

        readerGuard.Release();

        if (needToDrain) {
            auto writerGuard = WriterGuard(shard->SpinLock);
            shard->DrainTouchBuffer();
        }

        return valueFuture;
    }

    auto valueIt = valueMap.find(key);
    if (valueIt == valueMap.end()) {
        return {};
    }

    auto value = DangerousGetPtr(valueIt->second);
    if (!value) {
        return {};
    }

    readerGuard.Release();

    auto writerGuard = WriterGuard(shard->SpinLock);

    if (auto itemIt = itemMap.find(key); itemIt != itemMap.end()) {
        auto* item = itemIt->second;

        shard->Touch(item);
        auto valueFuture = item->GetValueFuture();

        if (item->Value) {
            SyncHitWeightCounter_.Increment(item->CachedWeight);
            SyncHitCounter_.Increment();
        } else {
            AsyncHitCounter_.Increment();
            item->AsyncHitCount.fetch_add(1);
        }

        shard->DrainTouchBuffer();

        return valueFuture;
    }

    shard->DrainTouchBuffer();

    {
        auto* item = new TItem(value);
        value->Item_ = item;

        auto valueFuture = item->GetValueFuture();

        YT_VERIFY(itemMap.emplace(key, item).second);
        ++Size_;

        i64 weight = GetWeight(item->Value);
        shard->PushToYounger(item, weight);
        SyncHitWeightCounter_.Increment(weight);
        SyncHitCounter_.Increment();

        // NB: Releases the lock.
        NotifyOnTrim(shard->Trim(writerGuard), value);

        return valueFuture;
    }
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::BeginInsert(const TKey& key) -> TInsertCookie
{
    auto* shard = GetShardByKey(key);

    if (auto valueFuture = DoLookup(shard, key)) {
        return TInsertCookie(
            key,
            nullptr,
            std::move(valueFuture),
            false);
    }

    while (true) {
        auto guard = WriterGuard(shard->SpinLock);

        shard->DrainTouchBuffer();

        auto& itemMap = shard->ItemMap;
        auto& valueMap = shard->ValueMap;

        auto itemIt = itemMap.find(key);
        if (itemIt != itemMap.end()) {
            auto* item = itemIt->second;
            shard->Touch(item);
            auto valueFuture = item->GetValueFuture();

            if (item->Value) {
                SyncHitWeightCounter_.Increment(item->CachedWeight);
                SyncHitCounter_.Increment();
            } else {
                AsyncHitCounter_.Increment();
                item->AsyncHitCount.fetch_add(1);
            }

            return TInsertCookie(
                key,
                nullptr,
                std::move(valueFuture),
                false);
        }

        auto valueIt = valueMap.find(key);
        if (valueIt == valueMap.end()) {
            auto* item = new TItem();
            auto valueFuture = item->GetValueFuture();

            YT_VERIFY(itemMap.emplace(key, item).second);
            ++Size_;

            MissedCounter_.Increment();

            return TInsertCookie(
                key,
                this,
                std::move(valueFuture),
                true);
        }

        if (auto value = DangerousGetPtr(valueIt->second)) {
            auto* item = new TItem(value);
            value->Item_ = item;

            YT_VERIFY(itemMap.emplace(key, item).second);
            ++Size_;

            i64 weight = GetWeight(item->Value);
            shard->PushToYounger(item, weight);
            SyncHitWeightCounter_.Increment(weight);
            SyncHitCounter_.Increment();

            // NB: Releases the lock.
            NotifyOnTrim(shard->Trim(guard), value);

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

    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    value->Cache_ = MakeWeak(this);

    auto* item = GetOrCrash(shard->ItemMap, key);
    item->Value = value;
    value->Item_ = item;
    auto promise = item->ValuePromise;

    YT_VERIFY(shard->ValueMap.emplace(key, value.Get()).second);

    i64 weight = GetWeight(item->Value);
    shard->PushToYounger(item, weight);
    // MissedCounter_ and AsyncHitCounter_ have already been incremented in BeginInsert.
    MissedWeightCounter_.Increment(weight);
    AsyncHitWeightCounter_.Increment(weight * item->AsyncHitCount.load());

    // NB: Releases the lock.
    NotifyOnTrim(shard->Trim(guard), value);

    promise.Set(value);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::CancelInsert(const TKey& key, const TError& error)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto& itemMap = shard->ItemMap;
    auto itemIt = itemMap.find(key);
    YT_VERIFY(itemIt != itemMap.end());

    auto* item = itemIt->second;
    auto promise = item->ValuePromise;

    itemMap.erase(itemIt);
    --Size_;

    YT_VERIFY(!item->Value);

    delete item;

    guard.Release();

    promise.Set(error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    YT_VERIFY(shard->ItemMap.find(key) == shard->ItemMap.end());
    YT_VERIFY(shard->ValueMap.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key, bool forbidResurrection)
{
    DoTryRemove(key, nullptr, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemoveValue(const TValuePtr& value, bool forbidResurrection)
{
    DoTryRemove(value->GetKey(), value, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::DoTryRemove(
    const TKey& key,
    const TValuePtr& value,
    bool forbidResurrection)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto& itemMap = shard->ItemMap;
    auto& valueMap = shard->ValueMap;

    auto valueIt = valueMap.find(key);
    if (valueIt == valueMap.end()) {
        return;
    }

    if (value && valueIt->second != value) {
        return;
    }

    if (forbidResurrection || !IsResurrectionSupported()) {
        valueIt->second->Cache_.Reset();
        valueMap.erase(valueIt);
    }

    auto itemIt = itemMap.find(key);
    if (itemIt == itemMap.end()) {
        return;
    }

    auto* item = itemIt->second;
    auto actualValue = item->Value;
    if (!actualValue) {
        return;
    }

    itemMap.erase(itemIt);
    --Size_;

    shard->PopFromLists(item);

    YT_VERIFY(actualValue->Item_ == item);
    actualValue->Item_ = nullptr;

    delete item;

    guard.Release();

    OnRemoved(actualValue);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        return;
    }

    auto item = itemIt->second;
    if (!item->Value) {
        return;
    }

    i64 newWeight = GetWeight(item->Value);
    i64 weightDelta = newWeight - item->CachedWeight;

    shard->UpdateWeight(item, weightDelta);

    // If item weight increases, it means that some parts of the item were missing in cache,
    // so add delta to missed weight.
    if (weightDelta > 0) {
        MissedWeightCounter_.Increment(weightDelta);
    }

    NotifyOnTrim(shard->Trim(guard), nullptr);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TValuePtr& value)
{
    UpdateWeight(value->GetKey());
}


template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::GetShardByKey(const TKey& key) const -> TShard*
{
    return &Shards_[THash()(key) & (Config_->ShardCount - 1)];
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
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::Trim(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard)
{
    auto evictedItems = this->TrimNoDelete();

    Parent->Size_ -= static_cast<int>(evictedItems.Size());

    std::vector<TValuePtr> evictedValues;
    for (const auto& item : evictedItems) {
        auto value = item.Value;

        YT_VERIFY(ItemMap.erase(value->GetKey()) == 1);

        if (!Parent->IsResurrectionSupported()) {
            YT_VERIFY(ValueMap.erase(value->GetKey()) == 1);
            value->Cache_.Reset();
        }

        YT_VERIFY(value->Item_ == &item);
        value->Item_ = nullptr;

        evictedValues.push_back(std::move(value));
    }

    // NB. Evicted items must die outside of critical section.
    guard.Release();

    return evictedValues;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::OnYoungerUpdated(i64 deltaCount, i64 deltaWeight)
{
    Parent->YoungerSizeCounter_ += deltaCount;
    Parent->YoungerWeightCounter_ += deltaWeight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::OnOlderUpdated(i64 deltaCount, i64 deltaWeight)
{
    Parent->OlderSizeCounter_ += deltaCount;
    Parent->OlderWeightCounter_ += deltaWeight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::NotifyOnTrim(
    const std::vector<TValuePtr>& evictedValues,
    const TValuePtr& insertedValue)
{
    if (insertedValue) {
        OnAdded(insertedValue);
    }
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
    Cancel(TError(NYT::EErrorCode::Canceled, "Cache item insertion aborted"));
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::TMemoryTrackingAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase<TKey, TValue, THash>(
        std::move(config),
        profiler)
    , MemoryTracker_(std::move(memoryTracker))
{
    MemoryTracker_->SetLimit(this->GetCapacity());
}

template <class TKey, class TValue, class THash>
TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::~TMemoryTrackingAsyncSlruCacheBase()
{
    MemoryTracker_->SetLimit(0);
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& value)
{
    MemoryTracker_->Acquire(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& value)
{
    MemoryTracker_->Release(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    if (auto newCapacity = config->Capacity) {
        MemoryTracker_->SetLimit(*newCapacity);
    }
    TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
