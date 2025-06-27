#ifndef ENTITY_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include entity_map.h"
// For the sake of sane code completion.
#include "entity_map.h"
#endif

#include "serialize.h"

#include <yt/yt/core/misc/pool_allocator.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/chunked_input_stream.h>
#include <library/cpp/yt/memory/chunked_output_stream.h>

#include <util/system/type_name.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TEntityMapSaveBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
int TDefaultEntityMapTraits<TValue>::GetParallelSaveBatchSize() const
{
    return 1'000;
}

template <class TValue>
std::unique_ptr<TValue> TDefaultEntityMapTraits<TValue>::Create(const TEntityKey<TValue>& key) const
{
    if constexpr(std::derived_from<TValue, TPoolAllocator::TObjectBase>) {
        return TPoolAllocator::New<TValue>(key);
    } else {
        return std::make_unique<TValue>(key);
    }
}

////////////////////////////////////////////////////////////////////////////////

inline TEntityDynamicDataBase* TEntityBase::GetDynamicData() const
{
    return DynamicData_;
}

inline void TEntityBase::SetDynamicData(TEntityDynamicDataBase* data)
{
    DynamicData_ = data;
}

template <class T>
inline T* TEntityBase::GetTypedDynamicData() const
{
    return static_cast<T*>(DynamicData_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TReadOnlyEntityMap<TValue>::TIterator::TIterator()
    : Iterator_(nullptr)
{ }

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator*() const -> const std::pair<const TKey, TValue*>&
{
    return *Iterator_;
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator->() const -> const std::pair<const TKey, TValue*>*
{
    return Iterator_.operator->();
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator++() -> TIterator&
{
    ++Iterator_;
    return *this;
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator--() -> TIterator&
{
    --Iterator_;
    return *this;
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator++(int) -> TIterator
{
    auto old = *this;
    ++Iterator_;
    return old;
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator--(int) -> TIterator
{
    auto old = *this;
    --Iterator_;
    return old;
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::TIterator::operator==(const TIterator& other) const
{
    return Iterator_ == other.Iterator_;
}

template <class TValue>
TReadOnlyEntityMap<TValue>::TIterator::TIterator(typename TMapType::const_iterator iterator)
    : Iterator_(std::move(iterator))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TValue* TReadOnlyEntityMap<TValue>::Find(const TKey& key) const
{
    auto it = Map_.find(key);
    return it == Map_.end() ? nullptr : it->second;
}

template <class TValue>
TValue* TReadOnlyEntityMap<TValue>::Get(const TKey& key) const
{
    auto* value = Find(key);
    YT_VERIFY(value);
    return value;
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::Contains(const TKey& key) const
{
    return Find(key);
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::Begin() const
{
    return TIterator(Map_.begin());
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::End() const
{
    return TIterator(Map_.end());
}

template <class TValue>
i64 TReadOnlyEntityMap<TValue>::GetSize() const
{
    return std::ssize(Map_);
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::begin() const
{
    return Begin();
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::end() const
{
    return End();
}

template <class TValue>
size_t TReadOnlyEntityMap<TValue>::size() const
{
    return GetSize();
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::empty() const
{
    return Map_.empty();
}

////////////////////////////////////////////////////////////////////////////////

struct TDynamicEntityDataTag
{ };

template <class TValue>
TMutableEntityMap<TValue>::TMutableEntityMap()
    : DynamicDataPool_(TDynamicEntityDataTag())
{ }

template <class TValue>
TMutableEntityMap<TValue>::~TMutableEntityMap()
{
    DoClear();
}

template <class TValue>
TValue* TMutableEntityMap<TValue>::Insert(const TKey& key, std::unique_ptr<TValue> valueHolder)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    auto* value = valueHolder.release();
    YT_ASSERT(value);

    YT_VERIFY(this->Map_.emplace(key, value).second);
    value->SetDynamicData(AllocateDynamicData());

    return value;
}

template <class TValue>
void TMutableEntityMap<TValue>::Remove(const TKey& key)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    YT_VERIFY(TryRemove(key));
}

template <class TValue>
bool TMutableEntityMap<TValue>::TryRemove(const TKey& key)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    auto it = this->Map_.find(key);
    if (it == this->Map_.end()) {
        return false;
    }

    auto* value = it->second;
    FreeDynamicData(value->GetDynamicData());
    delete value;
    this->Map_.erase(it);
    return true;
}

template <class TValue>
std::unique_ptr<TValue> TMutableEntityMap<TValue>::Release(const TKey& key)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    auto it = this->Map_.find(key);
    YT_ASSERT(it != this->Map_.end());
    auto* value = it->second;
    FreeDynamicData(value->GetDynamicData());
    value->SetDynamicData(nullptr);
    this->Map_.erase(it);
    return std::unique_ptr<TValue>(value);
}

template <class TValue>
void TMutableEntityMap<TValue>::Clear()
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    DoClear();
}

template <class TValue>
void TMutableEntityMap<TValue>::DoClear()
{
    for (const auto& [key, entity] : this->Map_) {
        FreeDynamicData(entity->GetDynamicData());
        delete entity;
    }
    this->Map_.clear();
    DynamicDataPool_.Clear();
    FirstSpareDynamicData_ = nullptr;
}

template <class TValue>
auto TMutableEntityMap<TValue>::AllocateDynamicData() -> TDynamicData*
{
    TDynamicData* data;
    if (FirstSpareDynamicData_) {
        data = reinterpret_cast<TDynamicData*>(FirstSpareDynamicData_);
        FirstSpareDynamicData_ = FirstSpareDynamicData_->Next;
    } else {
        data = reinterpret_cast<TDynamicData*>(DynamicDataPool_.AllocateAligned(
            std::max(sizeof(TDynamicData), sizeof(TSpareEntityDynamicData))));
    }
    new(data) TDynamicData();
    return data;
}

template <class TValue>
void TMutableEntityMap<TValue>::FreeDynamicData(TDynamicData* data)
{
    data->TDynamicData::~TDynamicData();
    auto* spareData = reinterpret_cast<TSpareEntityDynamicData*>(data);
    spareData->Next  = FirstSpareDynamicData_;
    FirstSpareDynamicData_ = spareData;
}

template <class TValue>
std::string TMutableEntityMap<TValue>::GetTypeName()
{
    return CppDemangle(typeid(TValue).name());
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class TTraits>
TEntityMap<TValue, TTraits>::TEntityMap(const TTraits& traits)
    : Traits_(traits)
{ }

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::SaveKeys(TContext& context) const
{
    SaveIterators_.clear();

    if (auto backgroundInvoker = context.GetBackgroundInvoker()) {
        constexpr size_t Parallelism = 256;
        std::array<int, Parallelism> counters{};
        for (auto it = this->Map_.begin(); it != this->Map_.end(); ++it) {
            ++counters[TEntityHash<TValue>()(it->first) % Parallelism];
        }

        std::array<int, Parallelism> currentPositions;
        currentPositions[0] = 0;
        for (size_t index = 1; index < Parallelism; ++index) {
            currentPositions[index] = currentPositions[index - 1] + counters[index - 1];
        }

        auto startPositions = currentPositions;

        SaveIterators_.resize(this->Map_.size());
        for (auto it = this->Map_.begin(); it != this->Map_.end(); ++it) {
            auto position = currentPositions[TEntityHash<TValue>()(it->first) % Parallelism]++;
            SaveIterators_[position] = it;
        }

        const auto& endPositions = currentPositions;

        std::vector<TFuture<void>> futures;
        for (size_t index = 0; index < Parallelism; ++index) {
            futures.push_back(
                BIND([this, startPosition = startPositions[index], endPosition = endPositions[index]] {
                    std::sort(
                        SaveIterators_.begin() + startPosition,
                        SaveIterators_.begin() + endPosition,
                        [] (auto lhs, auto rhs) { return lhs->first < rhs->first; });
                })
                    .AsyncVia(backgroundInvoker)
                    .Run());
        }

        AllSucceeded(std::move(futures))
            .Get()
            .ThrowOnError();
    } else {
        SaveIterators_.reserve(this->Map_.size());
        for (auto it = this->Map_.begin(); it != this->Map_.end(); ++it) {
            SaveIterators_.push_back(it);
        }

        std::sort(
            SaveIterators_.begin(),
            SaveIterators_.end(),
            [] (auto lhs, auto rhs) { return lhs->first < rhs->first; });
    }

    TSizeSerializer::Save(context, SaveIterators_.size());
    for (const auto& it : SaveIterators_) {
        Save(context, it->first);
        it->second->GetDynamicData()->SerializationKey = context.GenerateSerializationKey();
    }
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::SaveValues(TContext& context) const
{
    if (!SaveIterators_.empty()) {
        Save(context, AllEntitiesBatchEntityCount);
    }

    for (const auto& it : SaveIterators_) {
        Save(context, *it->second);
    }
    SaveIterators_.clear();
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::SaveValuesParallel(TContext& context) const
{
    auto backgroundInvoker = context.GetBackgroundInvoker();
    if (!backgroundInvoker) {
        SaveValues(context);
        return;
    }

    int batchSize = Traits_.GetParallelSaveBatchSize();

    struct TBatchResult
    {
        int EntityCount;
        size_t ByteSize;
        std::vector<TSharedRef> Chunks;
    };

    std::vector<TBatchResult> batchResults;
    std::vector<TCallback<TFuture<void>()>> batchExecutors;
    int entityStartIndex = 0;
    while (entityStartIndex < std::ssize(SaveIterators_)) {
        int entityEndIndex = std::min<int>(entityStartIndex + batchSize, std::ssize(SaveIterators_));
        int batchIndex = std::ssize(batchExecutors);
        batchExecutors.push_back(BIND([this, &context, &batchResults, entityStartIndex, entityEndIndex, batchIndex] {
            TChunkedOutputStream batchOutput(GetRefCountedTypeCookie<TEntityMapSaveBufferTag>());
            TContext batchContext(&batchOutput, &context);
            for (int index = entityStartIndex; index < entityEndIndex; ++index) {
                Save(batchContext, *SaveIterators_[index]->second);
            }
            batchContext.Finish();
            batchResults[batchIndex] = {
                .EntityCount = entityEndIndex - entityStartIndex,
                .ByteSize = batchOutput.GetSize(),
                .Chunks = batchOutput.Finish(),
            };
        }).AsyncVia(backgroundInvoker));
        entityStartIndex = entityEndIndex;
    }

    int batchCount = std::ssize(batchExecutors);

    batchResults.resize(batchCount);
    std::vector<TFuture<void>> batchFutures(batchCount);

    int batchIndexToStart = 0;
    int batchesRunning = 0;

    auto startMoreBatches = [&] {
        while (batchIndexToStart < batchCount && batchesRunning < context.GetBackgroundParallelism()) {
            YT_VERIFY(!batchFutures[batchIndexToStart]);
            batchFutures[batchIndexToStart] = batchExecutors[batchIndexToStart]();
            ++batchesRunning;
            ++batchIndexToStart;
        }
    };

    int batchIndexToWaitFor = 0;
    auto waitForBatch = [&] {
        if (batchIndexToWaitFor >= batchCount) {
            return false;
        }
        batchFutures[batchIndexToWaitFor]
            .Get()
            .ThrowOnError();
        --batchesRunning;

        auto batchResult = std::move(batchResults[batchIndexToWaitFor]);

        Save(context, batchResult.EntityCount);
        TSizeSerializer::Save(context, batchResult.ByteSize);

        for (const auto& chunk : batchResult.Chunks) {
            context.GetOutput()->Write(chunk.Begin(), chunk.Size());
        }
        ++batchIndexToWaitFor;
        return true;
    };

    do {
        startMoreBatches();
    } while (waitForBatch());

    SaveIterators_.clear();

}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::LoadKeys(TContext& context)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    this->Clear();

    TStreamLoadContextScopeGuard scopeGuard(context, Format("keys:%v", this->GetTypeName()));

    size_t size;
    auto value = TSizeSerializer::Load(context);
    // COMPAT(danilalexeev): YT-24017.
    if (value == BatchedFormatMarker) {
        size = TSizeSerializer::LoadSuspended(context);
    } else {
        size = value;
    }

    SERIALIZATION_DUMP_WRITE(context, "keys[%v]", size);

    LoadKeys_.clear();
    LoadKeys_.reserve(size);
    LoadValues_.clear();
    LoadValues_.reserve(size);

    this->Map_.reserve(size);

    SERIALIZATION_DUMP_INDENT(context) {
        for (size_t index = 0; index < size; ++index) {
            auto key = LoadSuspended<TKey>(context);
            LoadKeys_.push_back(key);

            auto value = Traits_.Create(key);
            LoadValues_.push_back(value.get());

            auto serializationKey = context.RegisterRawEntity(value.get());

            value->SetDynamicData(this->AllocateDynamicData());

            YT_VERIFY(this->Map_.emplace(key, value.release()).second);

            SERIALIZATION_DUMP_WRITE(context, "%v aka %v", key, serializationKey);
        }
    }
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::LoadValues(TContext& context, std::optional<int> firstBatchEntityCount)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    YT_VERIFY(LoadKeys_.size() == LoadValues_.size());

    auto finally = Finally([&] {
        LoadKeys_.clear();
        LoadValues_.clear();
    });

    TStreamLoadContextScopeGuard mapScopeGuard(context, Format("values:%v", this->GetTypeName()));

    SERIALIZATION_DUMP_WRITE(context, "values[%v]", LoadKeys_.size());

    int batchEntityCount = AllEntitiesBatchEntityCount;
    if (firstBatchEntityCount) {
        batchEntityCount = *firstBatchEntityCount;
    } else if (!LoadKeys_.empty()) {
        Load(context, batchEntityCount);
    }

    bool areSizePrefixesPresent = batchEntityCount != AllEntitiesBatchEntityCount;
    if (batchEntityCount == AllEntitiesBatchEntityCount) {
        batchEntityCount = std::ssize(LoadKeys_);
    }

    SERIALIZATION_DUMP_INDENT(context) {
        int entityStartIndex = 0;
        while (entityStartIndex < std::ssize(LoadKeys_)) {
            int entityEndIndex = std::min<int>(entityStartIndex + batchEntityCount, std::ssize(LoadKeys_));

            if (areSizePrefixesPresent) {
                if (entityStartIndex > 0) {
                    int dumpBatchEntityCount;
                    Load(context, dumpBatchEntityCount);
                }
                TSizeSerializer::Load(context);
            }

            for (int index = entityStartIndex; index < entityEndIndex; ++index) {
                const auto& key = LoadKeys_[index];

                TStreamLoadContextScopeGuard valueScopeGuard(context, ToString(key));

                SERIALIZATION_DUMP_WRITE(context, "%v =>", key);
                SERIALIZATION_DUMP_INDENT(context) {
                    Load(context, *LoadValues_[index]);
                }
            }

            entityStartIndex = entityEndIndex;
        }
    }
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::LoadValuesParallel(TContext& context)
{
    YT_ASSERT_THREAD_AFFINITY(this->UserThread);

    YT_VERIFY(LoadKeys_.size() == LoadValues_.size());

    auto finally = Finally([&] {
        LoadKeys_.clear();
        LoadValues_.clear();
    });

    int batchEntityCount = AllEntitiesBatchEntityCount;
    if (!LoadKeys_.empty()) {
        Load(context, batchEntityCount);
    }

    auto backgroundInvoker = context.GetBackgroundInvoker();
    if (!backgroundInvoker || batchEntityCount == AllEntitiesBatchEntityCount) {
        LoadValues(context, batchEntityCount);
        return;
    }

    int batchesRunning = 0;
    std::vector<TFuture<void>> batchFutures;

    int entityStartIndex = 0;
    auto startMoreBatches = [&] {
        while (entityStartIndex < std::ssize(LoadKeys_) && batchesRunning < context.GetBackgroundParallelism()) {
            if (entityStartIndex > 0) {
                Load(context, batchEntityCount);
            }
            auto batchByteSize = TSizeSerializer::Load(context);

            int entityEndIndex = std::min<int>(entityStartIndex + batchEntityCount, std::ssize(LoadKeys_));

            TChunkedOutputStream batchBuffer(GetRefCountedTypeCookie<TEntityMapSaveBufferTag>());
            auto* buffer = batchBuffer.Preallocate(batchByteSize);
            context.GetInput()->Load(buffer, batchByteSize);
            batchBuffer.Advance(batchByteSize);

            batchFutures.push_back(BIND(
                [this, &context, entityStartIndex, entityEndIndex] (std::vector<TSharedRef> batchChunks) {
                    auto batchInput = TChunkedInputStream(std::move(batchChunks));
                    auto batchContext = TContext(&batchInput, &context);

                    for (int index = entityStartIndex; index < entityEndIndex; ++index) {
                        Load(batchContext, *LoadValues_[index]);
                    }
                }).AsyncVia(backgroundInvoker).Run(batchBuffer.Finish()));

            ++batchesRunning;
            entityStartIndex = entityEndIndex;
        }
    };

    int batchIndexToWaitFor = 0;
    auto waitForBatch = [&] {
        if (batchIndexToWaitFor >= std::ssize(batchFutures)) {
            return false;
        }
        batchFutures[batchIndexToWaitFor]
            .Get()
            .ThrowOnError();
        --batchesRunning;
        ++batchIndexToWaitFor;
        return true;
    };

    do {
        startMoreBatches();
    } while (waitForBatch());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
