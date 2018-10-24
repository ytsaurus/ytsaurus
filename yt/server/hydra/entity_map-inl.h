#pragma once
#ifndef ENTITY_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include entity_map.h"
// For the sake of sane code completion.
#include "entity_map.h"
#endif

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
std::unique_ptr<TValue> TDefaultEntityMapTraits<TValue>::Create(const TEntityKey<TValue>& key) const
{
    return std::unique_ptr<TValue>(new TValue(key));
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
auto TReadOnlyEntityMap<TValue>::TIterator::operator*() const -> std::pair<const TKey, TValue*>
{
    return *Iterator_;
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
auto TReadOnlyEntityMap<TValue>::TIterator::operator++(int) -> TIterator&
{
    Iterator_++;
    return *this;
}

template <class TValue>
auto TReadOnlyEntityMap<TValue>::TIterator::operator--(int) -> TIterator&
{
    Iterator_--;
    return *this;
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::TIterator::operator==(const TIterator& other) const
{
    return Iterator_ == other.Iterator_;
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::TIterator::operator!=(const TIterator& other) const
{
    return Iterator_ != other.Iterator_;
}

template <class TValue>
TReadOnlyEntityMap<TValue>::TIterator::TIterator(typename TMapType::const_iterator iterator)
    : Iterator_(std::move(iterator))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TValue* TReadOnlyEntityMap<TValue>::Find(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map_.find(key);
    return it == Map_.end() ? nullptr : it->second;
}

template <class TValue>
TValue* TReadOnlyEntityMap<TValue>::Get(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto* value = Find(key);
    YCHECK(value);
    return value;
}

template <class TValue>
bool TReadOnlyEntityMap<TValue>::Contains(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Find(key);
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::Begin() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return TIterator(Map_.begin());
}

template <class TValue>
typename TReadOnlyEntityMap<TValue>::TIterator
TReadOnlyEntityMap<TValue>::End() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return TIterator(Map_.end());
}

template <class TValue>
int TReadOnlyEntityMap<TValue>::GetSize() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return static_cast<int>(Map_.size());
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

////////////////////////////////////////////////////////////////////////////////

struct TDynamicEntityDataTag
{ };

template <class TValue, class TTraits>
TEntityMap<TValue, TTraits>::TEntityMap(const TTraits& traits)
    : Traits_(traits)
    , DynamicDataPool_(TDynamicEntityDataTag())
{ }

template <class TValue, class TTraits>
TEntityMap<TValue, TTraits>::~TEntityMap()
{
    DoClear();
}

template <class TValue, class TTraits>
TValue* TEntityMap<TValue, TTraits>::Insert(const TKey& key, std::unique_ptr<TValue> valueHolder)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    auto* value = valueHolder.release();
    Y_ASSERT(value);

    YCHECK(this->Map_.insert(std::make_pair(key, value)).second);
    value->SetDynamicData(AllocateDynamicData());

    return value;
}

template <class TValue, class TTraits>
void TEntityMap<TValue, TTraits>::Remove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    YCHECK(TryRemove(key));
}

template <class TValue, class TTraits>
bool TEntityMap<TValue, TTraits>::TryRemove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

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

template <class TValue, class TTraits>
std::unique_ptr<TValue> TEntityMap<TValue, TTraits>::Release(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    auto it = this->Map_.find(key);
    Y_ASSERT(it != this->Map_.end());
    auto* value = it->second;
    FreeDynamicData(value->GetDynamicData());
    value->SetDynamicData(nullptr);
    this->Map_.erase(it);
    return std::unique_ptr<TValue>(value);
}

template <class TValue, class TTraits>
void TEntityMap<TValue, TTraits>::Clear()
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    DoClear();
}

template <class TValue, class TTraits>
void TEntityMap<TValue, TTraits>::DoClear()
{
    for (const auto& pair : this->Map_) {
        auto* entity = pair.second;
        FreeDynamicData(entity->GetDynamicData());
        delete entity;
    }
    this->Map_.clear();
    DynamicDataPool_.Clear();
    FirstSpareDynamicData_ = nullptr;
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::SaveKeys(TContext& context) const
{
    TSizeSerializer::Save(context, this->Map_.size());

    SaveIterators_.clear();
    SaveIterators_.reserve(this->Map_.size());
    for (auto it = this->Map_.begin(); it != this->Map_.end(); ++it) {
        SaveIterators_.push_back(it);
    }

    std::sort(
        SaveIterators_.begin(),
        SaveIterators_.end(),
        [] (const typename TMapType::const_iterator& lhs, const typename TMapType::const_iterator& rhs) {
            return lhs->first < rhs->first;
        });

    for (const auto& it : SaveIterators_) {
        Save(context, it->first);
        it->second->GetDynamicData()->SerializationKey = context.GenerateSerializationKey();
    }
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::SaveValues(TContext& context) const
{
    for (const auto& it : SaveIterators_) {
        Save(context, *it->second);
    }
    SaveIterators_.clear();
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::LoadKeys(TContext& context)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    Clear();

    size_t size = TSizeSerializer::LoadSuspended(context);

    SERIALIZATION_DUMP_WRITE(context, "keys[%v]", size);

    LoadKeys_.clear();
    LoadKeys_.reserve(size);
    LoadValues_.clear();
    LoadValues_.reserve(size);

    SERIALIZATION_DUMP_INDENT(context) {
        for (size_t index = 0; index < size; ++index) {
            auto key = LoadSuspended<TKey>(context);
            LoadKeys_.push_back(key);

            auto value = Traits_.Create(key);
            LoadValues_.push_back(value.get());

            auto serializationKey = context.RegisterEntity(value.get());

            value->SetDynamicData(AllocateDynamicData());

            YCHECK(this->Map_.insert(std::make_pair(key, value.release())).second);

            SERIALIZATION_DUMP_WRITE(context, "%v aka %v", key, serializationKey.Index);
        }
    }
}

template <class TValue, class TTraits>
template <class TContext>
void TEntityMap<TValue, TTraits>::LoadValues(TContext& context)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    YCHECK(LoadKeys_.size() == LoadValues_.size());

    SERIALIZATION_DUMP_WRITE(context, "values[%v]", LoadKeys_.size());

    SERIALIZATION_DUMP_INDENT(context) {
        for (size_t index = 0; index != LoadKeys_.size(); ++index) {
            SERIALIZATION_DUMP_WRITE(context, "%v =>", LoadKeys_[index]);
            SERIALIZATION_DUMP_INDENT(context) {
                Load(context, *LoadValues_[index]);
            }
        }
    }

    LoadKeys_.clear();
    LoadValues_.clear();
}

template <class TValue, class TTraits>
auto TEntityMap<TValue, TTraits>::AllocateDynamicData() -> TDynamicData*
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

template <class TValue, class TTraits>
void TEntityMap<TValue, TTraits>::FreeDynamicData(TDynamicData* data)
{
    data->TDynamicData::~TDynamicData();
    auto* spareData = reinterpret_cast<TSpareEntityDynamicData*>(data);
    spareData->Next  = FirstSpareDynamicData_;
    FirstSpareDynamicData_ = spareData;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
