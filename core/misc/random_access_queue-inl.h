#pragma once
#ifndef RANDOM_ACCESS_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include random_access_queue.h"
// For the sake of sane code completion.
#include "random_access_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
void TRandomAccessQueue<TKey, TValue>::Push(TRandomAccessQueue<TKey, TValue>::TEntry&& entry)
{
    auto key = entry.first;
    auto it = KeyToEntryMap_.find(key);
    if (it == KeyToEntryMap_.end()) {
        Queue_.push_back(std::move(entry));
        auto listIt = --Queue_.end();
        KeyToEntryMap_.insert(std::make_pair(std::move(key), listIt));
    } else {
        auto listIt = it->second;
        listIt->second = std::move(entry.second);
    }
}

template <class TKey, class TValue>
typename TRandomAccessQueue<TKey, TValue>::TEntry TRandomAccessQueue<TKey, TValue>::Pop()
{
    auto listIt = Queue_.begin();
    YCHECK(listIt != Queue_.end());
    auto entry = std::move(*listIt);
    KeyToEntryMap_.erase(entry.first);
    Queue_.erase(listIt);
    return entry;
}

template <class TKey, class TValue>
std::optional<typename TRandomAccessQueue<TKey, TValue>::TEntry> TRandomAccessQueue<TKey, TValue>::Pop(
    const TKey& key)
{
    if (auto it = KeyToEntryMap_.find(key)) {
        auto listIt = it->second;
        auto entry = std::move(*listIt);
        KeyToEntryMap_.erase(it);
        Queue_.erase(listIt);
        return entry;
    } else {
        return std::nullopt;
    }
}

template <class TKey, class TValue>
i64 TRandomAccessQueue<TKey, TValue>::Size() const
{
    return Queue_.size();
}

template <class TKey, class TValue>
bool TRandomAccessQueue<TKey, TValue>::IsEmpty() const
{
    return Queue_.empty();
}

template <class TKey, class TValue>
void TRandomAccessQueue<TKey, TValue>::Clear()
{
    Queue_.clear();
    KeyToEntryMap_.clear();
}

template <class TKey, class TValue>
template <class TSaveContext>
void TRandomAccessQueue<TKey, TValue>::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Queue_);
}

template <class TKey, class TValue>
template <class TLoadContext>
void TRandomAccessQueue<TKey, TValue>::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Queue_);

    for (auto it = Queue_.begin(); it != Queue_.end(); ++it) {
        YCHECK(KeyToEntryMap_.insert(std::make_pair(it->first, it)).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

