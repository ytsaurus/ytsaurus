#ifndef ENTITY_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include entity_map.h"
#endif
#undef ENTITY_MAP_INL_H_

#include <core/misc/serialize.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
std::unique_ptr<TValue> TDefaultEntityMapTraits<TKey, TValue>::Create(const TKey& key) const
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

template <class TKey, class TValue, class THash>
std::pair<const TKey, TValue*> TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator*() const
{
    return *Iterator_;
}

template <class TKey, class TValue, class THash>
auto TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator++() -> TIterator&
{
    ++Iterator_;
    return *this;
}

template <class TKey, class TValue, class THash>
auto TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator--() -> TIterator&
{
    --Iterator_;
    return *this;
}

template <class TKey, class TValue, class THash>
auto TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator++(int) -> TIterator&
{
    Iterator_++;
    return *this;
}

template <class TKey, class TValue, class THash>
auto TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator--(int) -> TIterator&
{
    Iterator_--;
    return *this;
}

template <class TKey, class TValue, class THash>
bool TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator==(const TIterator& other) const
{
    return Iterator_ == other.Iterator_;
}

template <class TKey, class TValue, class THash>
bool TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::operator!=(const TIterator& other) const
{
    return Iterator_ != other.Iterator_;
}

template <class TKey, class TValue, class THash>
TReadOnlyEntityMap<TKey, TValue, THash>::TIterator::TIterator(typename TMap::const_iterator iterator)
    : Iterator_(std::move(iterator))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TValue* TReadOnlyEntityMap<TKey, TValue, THash>::Find(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map_.find(key);
    return it == Map_.end() ? nullptr : it->second;
}

template <class TKey, class TValue, class THash>
TValue* TReadOnlyEntityMap<TKey, TValue, THash>::Get(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto* value = Find(key);
    YCHECK(value);
    return value;
}

template <class TKey, class TValue, class THash>
bool TReadOnlyEntityMap<TKey, TValue, THash>::Contains(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Find(key);
}

template <class TKey, class TValue, class THash>
typename TReadOnlyEntityMap<TKey, TValue, THash>::TIterator
TReadOnlyEntityMap<TKey, TValue, THash>::Begin() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return TIterator(Map_.begin());
}

template <class TKey, class TValue, class THash>
typename TReadOnlyEntityMap<TKey, TValue, THash>::TIterator
TReadOnlyEntityMap<TKey, TValue, THash>::End() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return TIterator(Map_.end());
}

template <class TKey, class TValue, class THash>
int TReadOnlyEntityMap<TKey, TValue, THash>::GetSize() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return static_cast<int>(Map_.size());
}

template <class TKey, class TValue, class THash>
typename TReadOnlyEntityMap<TKey, TValue, THash>::TIterator
TReadOnlyEntityMap<TKey, TValue, THash>::begin() const
{
    return Begin();
}

template <class TKey, class TValue, class THash>
typename TReadOnlyEntityMap<TKey, TValue, THash>::TIterator
TReadOnlyEntityMap<TKey, TValue, THash>::end() const
{
    return End();
}

template <class TKey, class TValue, class THash>
size_t TReadOnlyEntityMap<TKey, TValue, THash>::size() const
{
    return GetSize();
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializationKeysTag
{ };

template <class TKey, class TValue, class TTraits, class THash>
TEntityMap<TKey, TValue, TTraits, THash>::TEntityMap(const TTraits& traits)
    : Traits_(traits)
    , DynamicDataPool_(TSerializationKeysTag())
{ }

template <class TKey, class TValue, class TTraits, class THash>
TEntityMap<TKey, TValue, TTraits, THash>::~TEntityMap()
{
    Clear();
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Insert(const TKey& key, TValue* value)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    YASSERT(value);
    YCHECK(this->Map_.insert(std::make_pair(key, value)).second);
    value->SetDynamicData(AllocateDynamicData());
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Remove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    YCHECK(TryRemove(key));
}

template <class TKey, class TValue, class TTraits, class THash>
bool TEntityMap<TKey, TValue, TTraits, THash>::TryRemove(const TKey& key)
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

template <class TKey, class TValue, class TTraits, class THash>
std::unique_ptr<TValue> TEntityMap<TKey, TValue, TTraits, THash>::Release(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    auto it = this->Map_.find(key);
    YASSERT(it != this->Map_.end());
    auto* value = it->second;
    FreeDynamicData(value->GetDynamicData());
    value->SetDynamicData(nullptr);
    this->Map_.erase(it);
    return std::unique_ptr<TValue>(value);
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Clear()
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    for (const auto& pair : this->Map_) {
        auto* entity = pair.second;
        FreeDynamicData(entity->GetDynamicData());
        delete entity;
    }
    this->Map_.clear();
    DynamicDataPool_.Clear();
    FirstSpareDynamicData_ = nullptr;
}

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::SaveKeys(TContext& context) const
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
        [] (const typename TMap::const_iterator& lhs, const typename TMap::const_iterator& rhs) {
            return lhs->first < rhs->first;
        });

    for (const auto& it : SaveIterators_) {
        Save(context, it->first);
        it->second->GetDynamicData()->SerializationKey = context.GenerateSerializationKey();
    }
}

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::SaveValues(TContext& context) const
{
    for (const auto& it : SaveIterators_) {
        Save(context, *it->second);
    }
    SaveIterators_.clear();
}

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::LoadKeys(TContext& context)
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

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::LoadValues(TContext& context)
{
    VERIFY_THREAD_AFFINITY(this->UserThread);

    YCHECK(LoadKeys_.size() == LoadValues_.size());

    SERIALIZATION_DUMP_WRITE(context, "values[%v]", LoadKeys_.size());

    SERIALIZATION_DUMP_INDENT(context) {
        for (size_t index = 0; index < LoadKeys_.size(); ++index) {
            SERIALIZATION_DUMP_WRITE(context, "%v =>", LoadKeys_[index]);
            SERIALIZATION_DUMP_INDENT(context) {
                Load(context, *LoadValues_[index]);
            }
        }
    }

    LoadKeys_.clear();
    LoadValues_.clear();
}

template <class TKey, class TValue, class TTraits, class THash>
auto TEntityMap<TKey, TValue, TTraits, THash>::AllocateDynamicData() -> TDynamicData*
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

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::FreeDynamicData(TDynamicData* data)
{
    data->TDynamicData::~TDynamicData();
    auto* spareData = reinterpret_cast<TSpareEntityDynamicData*>(data);
    spareData->Next  = FirstSpareDynamicData_;
    FirstSpareDynamicData_ = spareData;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
