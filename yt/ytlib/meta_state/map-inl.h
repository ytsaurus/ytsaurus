#ifndef MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include map.h"
#endif
#undef MAP_INL_H_

#include <ytlib/actions/action_util.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAutoPtr<TValue> TDefaultMetaMapTraits<TKey, TValue>::Clone(TValue* value) const
{
    return value->Clone();
}

template <class TKey, class TValue>
void TDefaultMetaMapTraits<TKey, TValue>::Save(TValue* value, TOutputStream* output) const
{
    value->Save(output);
}

template <class TKey, class TValue>
TAutoPtr<TValue> TDefaultMetaMapTraits<TKey, TValue>::Load(const TKey& key, TInputStream* input) const
{
    return TValue::Load(key, input);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class TTraits, class THash >
TMetaStateMap<TKey, TValue, TTraits, THash>::TMetaStateMap(TTraits traits)
    : Traits(traits)
    , Size(0)
{ }

template <class TKey, class TValue, class TTraits, class THash >
TMetaStateMap<TKey, TValue, TTraits, THash>::~TMetaStateMap()
{
    FOREACH (const auto& pair, Map) {
        delete pair.second;
    }
    Map.clear();
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::Insert(const TKey& key, TValue* value)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(value);
    YVERIFY(Map.insert(MakePair(key, value)).second);
    ++Size;
}

template <class TKey, class TValue, class TTraits, class THash >
const TValue* TMetaStateMap<TKey, TValue, TTraits, THash>::Find(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    return it == Map.end() ? NULL : it->second;
}

template <class TKey, class TValue, class TTraits, class THash >
TValue* TMetaStateMap<TKey, TValue, TTraits, THash>::FindForUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    return it == Map.end() ? NULL : it->second;
}

template <class TKey, class TValue, class TTraits, class THash >
const TValue& TMetaStateMap<TKey, TValue, TTraits, THash>::Get(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto* value = Find(key);
    YASSERT(value);
    return *value;
}

template <class TKey, class TValue, class TTraits, class THash >
TValue& TMetaStateMap<TKey, TValue, TTraits, THash>::GetForUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto* value = FindForUpdate(key);
    YASSERT(value);
    return *value;
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::Remove(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    auto it = Map.find(key);
    YASSERT(it != Map.end());
    delete it->second;
    Map.erase(it);
    --Size;
}

template <class TKey, class TValue, class TTraits, class THash >
bool TMetaStateMap<TKey, TValue, TTraits, THash>::Contains(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Find(key);
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::Clear()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    FOREACH(const auto& pair, Map) {
        delete pair.second;
    }
    Map.clear();
    Size = 0;
}

template <class TKey, class TValue, class TTraits, class THash >
int TMetaStateMap<TKey, TValue, TTraits, THash>::GetSize() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Size;
}

template <class TKey, class TValue, class TTraits, class THash >
yvector<TKey> TMetaStateMap<TKey, TValue, TTraits, THash>::GetKeys(size_t sizeLimit) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    yvector<TKey> keys;
    keys.reserve(Min(static_cast<size_t>(Size), sizeLimit));

    FOREACH(const auto& pair, Map) {
        keys.push_back(pair.first);
        if (keys.size() == sizeLimit) {
            break;
        }
    }
    
    return keys;
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::Begin()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.begin();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::End()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.end();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TConstIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::Begin() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.begin();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TConstIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::End() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    return Map.end();
}

template <class TKey, class TValue, class TTraits, class THash>
void TMetaStateMap<TKey, TValue, TTraits, THash>::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    Map.clear();
    Size = ::LoadSize(input);
    
    for (i32 index = 0; index < Size; ++index) {
        TKey key;
        ::Load(input, key);
        auto value = Traits.Load(key, input);
        YVERIFY(Map.insert(MakePair(key, value.Release())).second);
    }
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::Save(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    ::SaveSize(output, Map.size());

    yvector<TItem> items(Map.begin(), Map.end());
    std::sort(items.begin(), items.end());

    FOREACH(const auto& item, items) {
        ::Save(output, item.first);
        Traits.Save(item.second, output);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
