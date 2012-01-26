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
    switch (State)
    {
        case EState::LoadingSnapshot:
        case EState::SavingSnapshot:
            YUNREACHABLE();

        case EState::Normal:
        case EState::HasPendingChanges:
            FOREACH (const auto& pair, PrimaryMap) {
                delete pair.second;
            }
            PrimaryMap.clear();
            FOREACH (const auto& pair, PatchMap) {
                if (pair.second) {
                    delete pair.second;
                }
            }
            PatchMap.clear();
            break;
    }
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::Insert(const TKey& key, TValue* value)
{
    VERIFY_THREAD_AFFINITY(UserThread);
    YASSERT(value);

    switch (State) {
        case EState::SavingSnapshot: {
            auto patchIt = PatchMap.find(key);
            if (patchIt == PatchMap.end()) {
                YASSERT(PrimaryMap.find(key) == PrimaryMap.end());
                YVERIFY(PatchMap.insert(MakePair(key, value)).second);
            } else {
                YASSERT(!patchIt->second);
                patchIt->second = value;
            }
            break;
        }
        case EState::HasPendingChanges:
        case EState::Normal:
            MergeTempTablesIfNeeded();
            YVERIFY(PrimaryMap.insert(MakePair(key, value)).second);
            break;

        default:
            YUNREACHABLE();
    }
    ++Size;
}

template <class TKey, class TValue, class TTraits, class THash >
const TValue* TMetaStateMap<TKey, TValue, TTraits, THash>::Find(const TKey& key) const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    switch (State) {
        case EState::SavingSnapshot: {
            auto patchIt = PatchMap.find(key);
            if (patchIt != PatchMap.end()) {
                return patchIt->second;
            }
            break;
        }
        case EState::HasPendingChanges:
        case EState::Normal: // for consistency
            const_cast<TThis*>(this)->MergeTempTablesIfNeeded();
            break;

        default:
            YUNREACHABLE();
    }

    auto it = PrimaryMap.find(key);
    return it == PrimaryMap.end() ? NULL : it->second;
}

template <class TKey, class TValue, class TTraits, class THash >
TValue* TMetaStateMap<TKey, TValue, TTraits, THash>::FindForUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    switch (State) {
        case EState::HasPendingChanges:
        case EState::Normal: {
            MergeTempTablesIfNeeded();
            auto mapIt = PrimaryMap.find(key);
            return mapIt == PrimaryMap.end() ? NULL : mapIt->second;
        }
        case EState::SavingSnapshot: {
            auto patchIt = PatchMap.find(key);
            if (patchIt != PatchMap.end()) {
                return patchIt->second;
            }

            auto mapIt = PrimaryMap.find(key);
            if (mapIt == PrimaryMap.end()) {
                return NULL;
            }

            TValue* clonedValue = Traits.Clone(mapIt->second).Release();
            YVERIFY(PatchMap.insert(MakePair(key, clonedValue)).second);
            return clonedValue;
        }
        default:
            YUNREACHABLE();
    }
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

     switch (State) {
        case EState::HasPendingChanges:
        case EState::Normal: {
            MergeTempTablesIfNeeded();

            auto it = PrimaryMap.find(key);
            YASSERT(it != PrimaryMap.end());
            delete it->second;
            PrimaryMap.erase(it);
            break;
        }
        case EState::SavingSnapshot: {
            auto patchIt = PatchMap.find(key);
            auto mainIt = PrimaryMap.find(key);
            if (patchIt == PatchMap.end()) {
                YASSERT(mainIt != PrimaryMap.end());
                YVERIFY(PatchMap.insert(TItem(key, NULL)).second);
            } else {
                YASSERT(patchIt->second);
                delete patchIt->second;
                if (mainIt == PrimaryMap.end()) {
                    PatchMap.erase(patchIt);
                } else {
                    patchIt->second = NULL;
                }
            }
            break;
        }
        default:
            YUNREACHABLE();
    }
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

    switch (State) {
        case EState::HasPendingChanges:
        case EState::Normal: {
            MergeTempTablesIfNeeded();
            FOREACH(const auto& pair, PrimaryMap) {
                delete pair.second;
            }
            PrimaryMap.clear();
            break;
        }
        case EState::SavingSnapshot: {
            FOREACH (const auto& pair, PatchMap) {
                if (pair.second) {
                    delete pair.second;
                }
            }
            PatchMap.clear();

            FOREACH (const auto& pair, PrimaryMap) {
                PatchMap.insert(TItem(pair.first, NULL));
            }
            break;
        }
        default:
            YUNREACHABLE();
    }
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

    switch (State) {
        case EState::HasPendingChanges:
        case EState::Normal: {
            const_cast<TThis*>(this)->MergeTempTablesIfNeeded();
            FOREACH(const auto& pair, PrimaryMap) {
                keys.push_back(pair.first);
                if (keys.size() == sizeLimit) {
                    break;
                }
            }
            break;
        }
        case EState::SavingSnapshot: {
            FOREACH(const auto& pair, PrimaryMap) {
                auto patchIt = PatchMap.find(pair.first);
                if (patchIt == PatchMap.end() || patchIt->second) {
                    keys.push_back(pair.first);
                    if (keys.size() == sizeLimit) {
                        break;
                    }
                }
            }
            FOREACH(const auto& pair, PatchMap) {
                if (pair.second) {
                    auto primaryIt = PrimaryMap.find(pair.first);
                    if (primaryIt == PrimaryMap.end()) {
                        keys.push_back(pair.first);
                        if (keys.size() == sizeLimit) {
                            break;
                        }
                    }
                }
            }
            break;
        }
        default:
            YUNREACHABLE();
    }

    YASSERT(keys.ysize() == Size);
    return MoveRV(keys);
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::Begin()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
    MergeTempTablesIfNeeded();

    return PrimaryMap.begin();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::End()
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
    MergeTempTablesIfNeeded();

    return PrimaryMap.end();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TConstIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::Begin() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
    MergeTempTablesIfNeeded();

    return PrimaryMap.begin();
}

template <class TKey, class TValue, class TTraits, class THash >
typename TMetaStateMap<TKey, TValue, TTraits, THash>::TConstIterator
TMetaStateMap<TKey, TValue, TTraits, THash>::End() const
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
    MergeTempTablesIfNeeded();

    return PrimaryMap.end();
}

template <class TKey, class TValue, class TTraits, class THash>
void TMetaStateMap<TKey, TValue, TTraits, THash>::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    YASSERT(State == EState::Normal || State == EState::HasPendingChanges);

    PrimaryMap.clear();
    PatchMap.clear();
    State = EState::LoadingSnapshot;

    Size = ::LoadSize(input);
    
    for (i32 index = 0; index < Size; ++index) {
        TKey key;
        ::Load(input, key);
        auto value = Traits.Load(key, input);
        PrimaryMap.insert(MakePair(key, value.Release()));
    }

    State = EState::Normal;
}

template <class TKey, class TValue, class TTraits, class THash >
TFuture<TVoid>::TPtr TMetaStateMap<TKey, TValue, TTraits, THash>::Save(
    IInvoker::TPtr invoker,
    TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(UserThread);

    MergeTempTablesIfNeeded();
    YASSERT(State == EState::Normal);

    YASSERT(PatchMap.empty());
    State = EState::SavingSnapshot;

    return
        FromMethod(&TMetaStateMap::DoSave, this, output)
        ->AsyncVia(invoker)
        ->Do();
}

template <class TKey, class TValue, class TTraits, class THash >
TVoid TMetaStateMap<TKey, TValue, TTraits, THash>::DoSave(TOutputStream* output)
{
    ::SaveSize(output, PrimaryMap.size());

    yvector<TItem> items(PrimaryMap.begin(), PrimaryMap.end());
    std::sort(
        items.begin(),
        items.end(),
        [] (const typename TMap::value_type& lhs, const typename TMap::value_type& rhs) {
            return lhs.first < rhs.first;
        });

    FOREACH(const auto& item, items) {
        ::Save(output, item.first);
        Traits.Save(item.second, output);
    }

    State = EState::HasPendingChanges;
    return TVoid();
}

template <class TKey, class TValue, class TTraits, class THash >
void TMetaStateMap<TKey, TValue, TTraits, THash>::MergeTempTablesIfNeeded()
{
    if (State != EState::HasPendingChanges) return;

    FOREACH (const auto& pair, PatchMap) {
        auto* value = pair.second;
        auto mainIt = PrimaryMap.find(pair.first);
        if (value) {
            if (mainIt == PrimaryMap.end()) {
                YVERIFY(PrimaryMap.insert(MakePair(pair.first, value)).second);
            } else {
                delete mainIt->second;
                mainIt->second = value;
            }
        } else {
            YASSERT(mainIt != PrimaryMap.end());
            delete mainIt->second;
            PrimaryMap.erase(mainIt);
        }
    }
    PatchMap.clear();

    State = EState::Normal;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
