#pragma once

#ifndef MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include map.h"
// For the sake of sane code completion.
#include "map.h"
#endif
#undef MAP_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual>
TSyncMap<TKey, TValue, THash, TEqual>::TSyncMap()
    : Snapshot_(new TSnapshot{})
{ }

template <class TKey, class TValue, class THash, class TEqual>
TSyncMap<TKey, TValue, THash, TEqual>::~TSyncMap()
{
    delete Snapshot_.load();
}

template <class TKey, class TValue, class THash, class TEqual>
template <class TFindKey>
TValue* TSyncMap<TKey, TValue, THash, TEqual>::Find(const TFindKey& key)
{
    {
        auto snapshot = AcquireSnapshot();

        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            return &(it->second->Value);
        }

        if (!snapshot->Dirty) {
            return nullptr;
        }
    }

    auto guard = Guard(Lock_);

    OnMiss();

    auto snapshot = Snapshot_.load();

    // Do another lookup, in case dirty was promoted.
    if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
        return &(it->second->Value);
    }

    if (!snapshot->Dirty) {
        return nullptr;
    }

    if (auto it = DirtyMap_->find(key); it != DirtyMap_->end()) {
        return &(it->second->Value);
    }

    return nullptr;
}

template <class TKey, class TValue, class THash, class TEqual>
template <class TCtor, class TFindKey>
std::pair<TValue*, bool> TSyncMap<TKey, TValue, THash, TEqual>::FindOrInsert(const TFindKey& key, const TCtor& ctor)
{
    {
        auto snapshot = AcquireSnapshot();

        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            return {&(it->second->Value), false};
        }
    }

    auto guard = Guard(Lock_);

    auto snapshot = Snapshot_.load();
    if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
        OnMiss();
        return {&(it->second->Value), false};
    }

    if (snapshot->Dirty) {
        if (auto it = DirtyMap_->find(key); it != DirtyMap_->end()) {
            OnMiss();
            return {&(it->second->Value), false};
        }
    }

    if (!snapshot->Dirty) {
        DirtyMap_ = New<TMap>(*snapshot->Map);
        UpdateSnapshot(snapshot->Map, true);
    }

    auto [newIt, inserted] = DirtyMap_->emplace(key, New<TEntry>(ctor()));
    YT_VERIFY(inserted);
    return {&(newIt->second->Value), true};
}

template <class TKey, class TValue, class THash, class TEqual>
auto TSyncMap<TKey, TValue, THash, TEqual>::AcquireSnapshot() -> THazardPtr<TSnapshot>
{
    return THazardPtr<TSnapshot>::Acquire([&] {
        return Snapshot_.load();
    });
}

template <class TKey, class TValue, class THash, class TEqual>
void TSyncMap<TKey, TValue, THash, TEqual>::UpdateSnapshot(TIntrusivePtr<TMap> map, bool dirty)
{
    auto newSnapshot = new TSnapshot{std::move(map), dirty};
    auto oldSnapshot = Snapshot_.exchange(newSnapshot);
    ScheduleObjectDeletion(oldSnapshot, [] (void *ptr) {
        delete static_cast<TSnapshot*>(ptr);
    });
}

template <class TKey, class TValue, class THash, class TEqual>
void TSyncMap<TKey, TValue, THash, TEqual>::OnMiss()
{
    if (!DirtyMap_) {
        return;
    }

    Misses_++;
    if (Misses_ < DirtyMap_->size()) {
        return;
    }

    Misses_ = 0;
    UpdateSnapshot(std::move(DirtyMap_), false);
}

template <class TKey, class TValue, class THash, class TEqual>
template <class TFn>
void TSyncMap<TKey, TValue, THash, TEqual>::IterateReadOnly(const TFn& fn)
{
    auto snapshot = AcquireSnapshot();

    for (const auto& [key, entry] : *snapshot->Map) {
        fn(key, entry->Value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
