#pragma once
#ifndef INDEXED_VECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include indexed_vector.h"
#endif

#include <yt/core/misc/serialize.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TIndexedVector<T>::IsEmpty() const
{
    return Items_.empty();
}

template <class T>
size_t TIndexedVector<T>::GetSize() const
{
    return Items_.size();
}

template <class T>
void TIndexedVector<T>::Reserve(size_t size)
{
    Items_.reserve(size);
    ItemToIndex_.reserve(size);
}

template <class T>
void TIndexedVector<T>::Clear()
{
    Items_.clear();
    ItemToIndex_.clear();
}

template <class T>
void TIndexedVector<T>::PushBack(const T& item)
{
    size_t index = Items_.size();
    Items_.push_back(item);
    YCHECK(ItemToIndex_.emplace(item, index).second);
}

template <class T>
void TIndexedVector<T>::Remove(const T& item)
{
    auto it = ItemToIndex_.find(item);
    YCHECK(it != ItemToIndex_.end());
    size_t index = it->second;
    if (index != Items_.size() - 1) {
        std::swap(Items_[index], Items_.back());
        ItemToIndex_[Items_[index]] = index;
    }
    Items_.pop_back();
    ItemToIndex_.erase(it);
}

template <class T>
const T* TIndexedVector<T>::Begin() const
{
    return Items_.data();
}

template <class T>
const T* TIndexedVector<T>::End() const
{
    return Items_.data() + Items_.size();
}

template <class T>
const T* TIndexedVector<T>::begin() const
{
    return Begin();
}

template <class T>
const T* TIndexedVector<T>::end() const
{
    return End();
}

////////////////////////////////////////////////////////////////////////////////

template <
    class TItemSerializer = TDefaultSerializer
>
struct TIndexedVectorSerializer
{
    template <class T, class C>
    static void Save(C& context, const TIndexedVector<T>& vector)
    {
        TSizeSerializer::Save(context, vector.GetSize());
        for (const auto& item : vector) {
            TItemSerializer::Save(context, item);
        }
    }

    template <class T, class C>
    static void Load(C& context, TIndexedVector<T>& vector)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        vector.Clear();
        vector.Reserve(size);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    T item;
                    TItemSerializer::Load(context, item);
                    vector.PushBack(item);
                }
            }
        }
    }
};

template <class T, class C>
struct TSerializerTraits<TIndexedVector<T>, C, void>
{
    typedef TIndexedVectorSerializer<> TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
