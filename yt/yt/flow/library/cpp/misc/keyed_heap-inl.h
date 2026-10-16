#pragma once

#ifndef KEYED_HEAP_INL_H_
    #error "Direct inclusion of this file is not allowed, include bipartite_map.h"
    // For the sake of sane code completion.
    #include "keyed_heap.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////


template <class TKey, class TValue, class TComparator>
TKeyedHeap<TKey, TValue, TComparator>::TKeyedHeap(TComparator comparator)
    : Comparator_(std::move(comparator))
{ }

template <class TKey, class TValue, class TComparator>
size_t TKeyedHeap<TKey, TValue, TComparator>::Size() const
{
    return Heap_.size();
}

template <class TKey, class TValue, class TComparator>
bool TKeyedHeap<TKey, TValue, TComparator>::Empty() const
{
    return Heap_.empty();
}

template <class TKey, class TValue, class TComparator>
void TKeyedHeap<TKey, TValue, TComparator>::OnAssignHelper(size_t index)
{
    Heap_[index]->second.second = index;
}

template <class TKey, class TValue, class TComparator>
template <class TInsertKey, class TInsertValue>
void TKeyedHeap<TKey, TValue, TComparator>::Set(TInsertKey&& key, TInsertValue&& value)
{
    auto [it, success] = HashMap_.try_emplace(std::forward<TInsertKey>(key), std::forward<TInsertValue>(value), 0);
    if (success) {
        it->second.second = Heap_.size();
        Heap_.emplace_back(it);
    } else {
        it->second.first = std::forward<TInsertValue>(value);
    }
    AdjustHeapItem(
        Heap_.begin(),
        Heap_.end(),
        Heap_.begin() + it->second.second,
        [this] (const auto& left, const auto& right) {
            return Comparator_(left->second.first, right->second.first);
        },
        [this] (size_t index) {
            return OnAssignHelper(index);
        });
}

template <class TKey, class TValue, class TComparator>
template <class TEraseKey>
bool TKeyedHeap<TKey, TValue, TComparator>::Erase(const TEraseKey& key)
{
    auto it = HashMap_.find(key);
    if (it == HashMap_.end()) {
        return false;
    }

    auto index = it->second.second;
    ExtractHeap(
        Heap_.begin(),
        Heap_.end(),
        Heap_.begin() + index,
        [this] (const auto& left, const auto& right) {
            return Comparator_(left->second.first, right->second.first);
        },
        [this] (size_t index) {
            return OnAssignHelper(index);
        });

    Heap_.pop_back();
    HashMap_.erase(it);

    return true;
}

template <class TKey, class TValue, class TComparator>
void TKeyedHeap<TKey, TValue, TComparator>::Clear()
{
    HashMap_.clear();
    Heap_.clear();
}

template <class TKey, class TValue, class TComparator>
const TValue& TKeyedHeap<TKey, TValue, TComparator>::TopValue() const
{
    return Heap_.front()->second.first;
}

template <class TKey, class TValue, class TComparator>
const TKey& TKeyedHeap<TKey, TValue, TComparator>::TopKey() const
{
    return Heap_.front()->first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
