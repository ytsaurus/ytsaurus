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

template <class TKey, class TValue, class THash>
typename IReadOnlyEntityMap<TKey, TValue, THash>::TConstIterator IReadOnlyEntityMap<TKey, TValue, THash>::begin() const
{
    return Begin();
}

template <class TKey, class TValue, class THash>
typename IReadOnlyEntityMap<TKey, TValue, THash>::TConstIterator IReadOnlyEntityMap<TKey, TValue, THash>::end() const
{
    return End();
}

template <class TKey, class TValue, class THash>
size_t IReadOnlyEntityMap<TKey, TValue, THash>::size() const
{
    return GetSize();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class TTraits, class THash>
TEntityMap<TKey, TValue, TTraits, THash>::TEntityMap(const TTraits& traits)
    : Traits(traits)
{ }

template <class TKey, class TValue, class TTraits, class THash>
TEntityMap<TKey, TValue, TTraits, THash>::~TEntityMap()
{
    for (const auto& pair : Map) {
        delete pair.second;
    }
    Map.clear();
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Insert(const TKey& key, TValue* value)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(value);
    YCHECK(Map.insert(std::make_pair(key, value)).second);
}

template <class TKey, class TValue, class TTraits, class THash>
TValue* TEntityMap<TKey, TValue, TTraits, THash>::Find(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    return it == Map.end() ? NULL : it->second;
}

template <class TKey, class TValue, class TTraits, class THash>
TValue* TEntityMap<TKey, TValue, TTraits, THash>::Get(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto* value = Find(key);
    YCHECK(value);
    return value;
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Remove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YCHECK(TryRemove(key));
}

template <class TKey, class TValue, class TTraits, class THash>
bool TEntityMap<TKey, TValue, TTraits, THash>::TryRemove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    if (it == Map.end()) {
        return false;
    }
    delete it->second;
    Map.erase(it);
    return true;
}

template <class TKey, class TValue, class TTraits, class THash>
std::unique_ptr<TValue> TEntityMap<TKey, TValue, TTraits, THash>::Release(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    YASSERT(it != Map.end());
    auto* value = it->second;
    Map.erase(it);
    return std::unique_ptr<TValue>(value);
}

template <class TKey, class TValue, class TTraits, class THash>
bool TEntityMap<TKey, TValue, TTraits, THash>::Contains(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Find(key);
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::Clear()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    for (const auto& pair : Map) {
        delete pair.second;
    }
    Map.clear();
}

template <class TKey, class TValue, class TTraits, class THash>
int TEntityMap<TKey, TValue, TTraits, THash>::GetSize() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return static_cast<int>(Map.size());
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TIterator
TEntityMap<TKey, TValue, TTraits, THash>::Begin()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.begin();
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TIterator
TEntityMap<TKey, TValue, TTraits, THash>::End()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.end();
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TConstIterator
TEntityMap<TKey, TValue, TTraits, THash>::Begin() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.begin();
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TConstIterator
TEntityMap<TKey, TValue, TTraits, THash>::End() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.end();
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::LoadKeys(TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    Map.clear();
    size_t size = TSizeSerializer::Load(context);

    auto previousKey = TKey();
    for (size_t index = 0; index < size; ++index) {
        TKey key;
        Load(context, key);

        YCHECK(index == 0 || previousKey < key);

        previousKey = key;

        auto value = Traits.Create(key);
        YCHECK(Map.insert(std::make_pair(key, value.release())).second);
    }
}

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::LoadValues(TContext& context)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    std::vector<TKey> keys;
    keys.reserve(Map.size());
    for (const auto& pair : Map) {
        keys.push_back(pair.first);
    }
    std::sort(keys.begin(), keys.end());

    for (const auto& key : keys) {
        auto it = Map.find(key);
        YCHECK(it != Map.end());
        Load(context, *it->second);
    }
}

template <class TKey, class TValue, class TTraits, class THash>
void TEntityMap<TKey, TValue, TTraits, THash>::SaveKeys(TSaveContext& context) const
{
    TSizeSerializer::Save(context, Map.size());

    std::vector<TKey> keys;
    keys.reserve(Map.size());
    for (const auto& pair : Map) {
        keys.push_back(pair.first);
    }
    std::sort(keys.begin(), keys.end());

    for (const auto& key : keys) {
        Save(context, key);
    }
}

template <class TKey, class TValue, class TTraits, class THash>
template <class TContext>
void TEntityMap<TKey, TValue, TTraits, THash>::SaveValues(TContext& context) const
{
    std::vector<TItem> items(Map.begin(), Map.end());
    std::sort(
        items.begin(),
        items.end(),
        [] (const TItem& lhs, const TItem& rhs) {
            return lhs.first < rhs.first;
        });

    for (const auto& item : items) {
        Save(context, *item.second);
    }
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TIterator TEntityMap<TKey, TValue, TTraits, THash>::begin()
{
    return Begin();
}

template <class TKey, class TValue, class TTraits, class THash>
typename TEntityMap<TKey, TValue, TTraits, THash>::TIterator TEntityMap<TKey, TValue, TTraits, THash>::end()
{
    return End();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
