#pragma once

#include "common.h"

#include "../misc/enum.h"
#include "../misc/assert.h"
#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/thread_affinity.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// TODO: Create inl-file and move implementation there

// TODO: DECLARE_ENUM cannot be used in a template class.
class TMetaStateMapBase
    : private TNonCopyable
{
protected:
    /*!
     * Transitions
     * - Normal -> LoadingSnapshot,
     * - Normal -> SavingSnapshot,
     * - HasPendingChanges -> Normal
     * are performed from the user thread.
     *
     * Transitions
     * - LoadingSnapshot -> Normal,
     * - SavingSnapshot -> HasPendingChanges
     * are performed from the snapshot invoker.
     */
    DECLARE_ENUM(EState,
        (Normal)
        (LoadingSnapshot)
        (SavingSnapshot)
        (HasPendingChanges)
    );

    EState State;
};

template <class TValue>
struct TDefaultMetaMapTraits
{
    TAutoPtr<TValue> Clone(TValue* value) const
    {
        return value->Clone();
    }

    void Save(TValue* value, TOutputStream* output) const
    {
        value->Save(output);
    }

    TAutoPtr<TValue> Load(TInputStream* input) const
    {
        return TValue::Load(input);
    }
};

//! Snapshottable map used to store various meta-state tables.
/*!
 *  \tparam TKey Key type.
 *  \tparam TValue Value type.
 *  \tparam THash Hash function for keys.
 * 
 *  \note
 *  All public methods must be called from a single thread.
 *  Exceptions are #Begin and #End, see below.
 * 
 *  TODO: this is not true, write about Traits
 *  TValue type must have the following methods:
 *          TAutoPtr<TValue> Clone();
 *          void Save(TOutputStream* output);
 *          static TAutoPtr<TValue> Load(TInputStream* input);
 */
template <
    class TKey,
    class TValue,
    class TTraits = TDefaultMetaMapTraits<TValue>,
    class THash = ::THash<TKey>
>
class TMetaStateMap
    : protected TMetaStateMapBase
{
public:
    typedef TMetaStateMap<TKey, TValue, TTraits, THash> TThis;
    typedef yhash_map<TKey, TValue*, THash> TMap;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;

    explicit TMetaStateMap(
        TTraits traits = TTraits())
        : Traits(traits)
    { }

    ~TMetaStateMap()
    {
        switch (State)
        {
            case EState::LoadingSnapshot:
            case EState::SavingSnapshot:
                YUNREACHABLE();

            case EState::Normal:
            case EState::HasPendingChanges:
                FOREACH (const auto& pair, PrimaryMap) {
                    delete pair.Second();
                }
                PrimaryMap.clear();
                FOREACH (const auto& pair, PatchMap) {
                    if (pair.Second() != NULL) {
                        delete pair.Second();
                    }
                }
                PatchMap.clear();
                break;
        }
    }

    //! Inserts a key-value pair.
    /*!
     *  \param key A key to insert.
     *  \param value A value to insert.
     *  
     *  \note The map will own the value and will call "delete" for it  when time comes.
     *  \note Fails if the key is already in map.
     */
    void Insert(const TKey& key, TValue* value)
    {
        VERIFY_THREAD_AFFINITY(UserThread);
        YASSERT(value != NULL);

        switch (State) {
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt == PatchMap.end()) {
                    YASSERT(PrimaryMap.find(key) == PrimaryMap.end());
                    YVERIFY(PatchMap.insert(MakePair(key, value)).Second());
                } else {
                    YASSERT(patchIt->Second() == NULL);
                    patchIt->Second() = value;
                }
                break;
            }
            case EState::HasPendingChanges:
            case EState::Normal:
                MergeTempTablesIfNeeded();
                YVERIFY(PrimaryMap.insert(MakePair(key, value)).Second());
                break;

            default:
                YUNREACHABLE();
        }
    }

    //! Tries to find a value by its key. The returned value is read-only.
    /*!
     *  \param key A key.
     *  \return A pointer to the value if found, NULL otherwise.
     */
    const TValue* Find(const TKey& key) const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt != PatchMap.end()) {
                    return patchIt->Second();
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
        return it == PrimaryMap.end() ? NULL : it->Second();
    }

    //! Tries to find a value by its key.
    //! May return a modifiable copy if snapshot creation is in progress.
    /*!
     * \param key A key.
     * \return A pointer to the value if found, NULL otherwise.
     */
    TValue* FindForUpdate(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();
                auto mapIt = PrimaryMap.find(key);
                return mapIt == PrimaryMap.end() ? NULL : mapIt->Second();
            }
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt != PatchMap.end()) {
                    return patchIt->Second();
                }         

                auto mapIt = PrimaryMap.find(key);
                if (mapIt == PrimaryMap.end()) {
                    return NULL;
                }

                TValue* clonedValue = Traits.Clone(mapIt->Second()).Release();
                YVERIFY(PatchMap.insert(MakePair(key, clonedValue)).Second());
                return clonedValue;
            }
            default:
                YUNREACHABLE();
        }
    }

    //! Returns a read-only value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns A reference to the value.
     */
    const TValue& Get(const TKey& key) const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        auto* value = Find(key);
        YASSERT(value != NULL);
        return *value;
    }

    //! Returns a modifiable value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns A reference to the value.
     */
    TValue& GetForUpdate(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        auto* value = FindForUpdate(key);
        YASSERT(value != NULL);
        return *value;
    }

    //! Removes the key from the map and deletes the corresponding value.
    /*!
     *  \param A key.
     *  
     *  \note Fails if the key is not in the map.
     */
    void Remove(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

         switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();

                auto it = PrimaryMap.find(key);
                YASSERT(it != PrimaryMap.end());
                delete it->Second();
                PrimaryMap.erase(it);
                break;
            }
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                auto mainIt = PrimaryMap.find(key);
                if (patchIt == PatchMap.end()) {
                    YASSERT(mainIt != PrimaryMap.end());
                    YVERIFY(PatchMap.insert(TItem(key, NULL)).Second());
                } else {
                    YASSERT(patchIt->Second() != NULL);
                    delete patchIt->Second();
                    if (mainIt == PrimaryMap.end()) {
                        PatchMap.erase(patchIt);
                    } else {
                        patchIt->Second() = NULL;
                    }
                }
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    //! Checks whether the key exists in the map.
    /*!
     *  \param key A key.
     *  \return True iff the key exists in the map.
     */
    bool Contains(const TKey& key) const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        return Find(key) != NULL;
    }

    //! Clears the map.
    void Clear()
    {
        VERIFY_THREAD_AFFINITY(UserThread);

         switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();
                FOREACH(const auto& pair, PrimaryMap) {
                    delete pair.Second();
                }
                PrimaryMap.clear();
                break;
            }
            case EState::SavingSnapshot: {
                FOREACH (const auto& pair, PatchMap) {
                    if (pair.Second() != NULL) {
                        delete pair.Second();
                    }
                }
                PatchMap.clear();

                FOREACH (const auto& pair, PrimaryMap) {
                    PatchMap.insert(TItem(pair.First(), NULL));
                }
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    //! (Unordered) begin()-iterator.
    /*!
     *  \note
     *  This call is potentially dangerous! 
     *  The user must understand its semantics and call it at its own risk.
     *  Iteration is only possible when no snapshot is being created.
     *  A typical use-case is to iterate over the items right after reading a snapshot.
     */
    TIterator Begin()
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal);
        return PrimaryMap.begin();
    }

    //! (Unordered) end()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TIterator End()
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal);
        return PrimaryMap.end();
    }
    
    //! (Unordered) const begin()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TConstIterator Begin() const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal);
        return PrimaryMap.begin();
    }

    //! (Unordered) const end()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TConstIterator End() const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal);
        return PrimaryMap.end();
    }

    //! Asynchronously saves the map to the stream.
    /*!
     *  This method saves the snapshot of the map as it is seen at the moment of
     *  the invocation. All further updates are accepted but are kept in-memory.
     *  
     *  \param invoker Invoker used to perform the heavy lifting.
     *  \param stream Output stream.
     *  \return An asynchronous result indicating that the snapshot is saved.
     */
    TFuture<TVoid>::TPtr Save(
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

    //! Asynchronously loads the map from the stream.
    /*!
     * This method loads the snapshot of the map in the background and at some
     * moment in the future swaps current map with the loaded one.
     * \param invoker Invoker for actual heavy work.
     * \param stream Input stream.
     * \return Callback on successful load.
     */
    TFuture<TVoid>::TPtr Load(
        IInvoker::TPtr invoker,
        TInputStream* input)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
        
        PrimaryMap.clear();
        PatchMap.clear();
        State = EState::LoadingSnapshot;

        return
            FromMethod(&TMetaStateMap::DoLoad, this, input)
            ->AsyncVia(invoker)
            ->Do();
    }
    
private:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);
    
    //! When no shapshot is being written this is the actual map we're working with.
    //! When a snapshot is being created this map is kept read-only and
    //! #PathMap is used to store the changes.
    TMap PrimaryMap;

    //! "(key, NULL)" indicates that the key should be deleted.
    TMap PatchMap;

    TTraits Traits;
    
    typedef TPair<TKey, TValue*> TItem;

    TVoid DoSave(TOutputStream* output)
    {
        ::Save(output, static_cast<i32>(PrimaryMap.size()));

        yvector<TItem> items(PrimaryMap.begin(), PrimaryMap.end());
        Sort(
            items.begin(),
            items.end(),
            [] (const typename TMap::value_type& lhs, const typename TMap::value_type& rhs) {
                return lhs.First() < rhs.First();
            });

        FOREACH(const auto& item, items) {
            ::Save(output, item.First());
            Traits.Save(item.Second(), output);
        }
        
        State = EState::HasPendingChanges;
        return TVoid();
    }

    TVoid DoLoad(TInputStream* input)
    {
        i32 size;
        ::Load(input, size);
        YASSERT(size >= 0);

        for (i32 index = 0; index < size; ++index) {
            TKey key;
            ::Load(input, key);
            TValue* value = Traits.Load(input).Release();
            PrimaryMap.insert(MakePair(key, value));
        }

        State = EState::Normal;

        return TVoid();
    }

    void MergeTempTablesIfNeeded()
    {
        if (State != EState::HasPendingChanges) return;

        FOREACH (const auto& pair, PatchMap) {
            auto* value = pair.Second();
            auto mainIt = PrimaryMap.find(pair.First());
            if (value != NULL) {
                if (mainIt == PrimaryMap.end()) {
                    YVERIFY(PrimaryMap.insert(MakePair(pair.First(), value)).Second());
                } else {
                    delete mainIt->Second();
                    mainIt->Second() = value;
                }
            } else {
                YASSERT(mainIt != PrimaryMap.end());
                delete mainIt->Second();
                PrimaryMap.erase(mainIt);
            }
        }
        PatchMap.clear();

        State = EState::Normal;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT


namespace NYT {
namespace NForeach {

//! Provides a begin-like iterator for #FOREACH macro.
template<class TKey, class TValue, class THash>
inline auto Begin(NMetaState::TMetaStateMap<TKey, TValue, THash>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

//! Provides an end-like iterator for #FOREACH macro.
template<class TKey, class TValue, class THash>
inline auto End(NMetaState::TMetaStateMap<TKey, TValue, THash>& collection) -> decltype(collection.End())
{
    return collection.End();
}
 
} // namespace NForeach
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

#define METAMAP_ACCESSORS_DECL(entityName, entityType, idType) \
    const entityType* Find ## entityName(const idType& id) const; \
    entityType* Find ## entityName ## ForUpdate(const idType& id); \
    const entityType& Get ## entityName(const idType& id) const; \
    entityType& Get ## entityName ## ForUpdate(const idType& id); \
    yvector<const entityType*> Get ## entityName ## s();

#define METAMAP_ACCESSORS_IMPL(declaringType, entityName, entityType, idType, map) \
    const entityType* declaringType::Find ## entityName(const idType& id) const \
    { \
        return (map).Find(id); \
    } \
    \
    entityType* declaringType::Find ## entityName ## ForUpdate(const idType& id) \
    { \
        return (map).FindForUpdate(id); \
    } \
    \
    const entityType& declaringType::Get ## entityName(const idType& id) const \
    { \
        return (map).Get(id); \
    } \
    \
    entityType& declaringType::Get ## entityName ## ForUpdate(const idType& id) \
    { \
        return (map).GetForUpdate(id); \
    }

#define METAMAP_ACCESSORS_FWD(declaringType, entityName, entityType, idType, fwd) \
    const entityType* declaringType::Find ## entityName(const idType& id) const \
    { \
        return (fwd).Find ## entityName(id); \
    } \
    \
    entityType* declaringType::Find ## entityName ## ForUpdate(const idType& id) \
    { \
        return (fwd).Find ## entityName ## ForUpdate(id); \
    } \
    \
    const entityType& declaringType::Get ## entityName(const idType& id) const \
    { \
        return (fwd).Get ## entityName(id); \
    } \
    \
    entityType& declaringType::Get ## entityName ## ForUpdate(const idType& id) \
    { \
        return (fwd).Get ## entityName ## ForUpdate(id); \
    }

////////////////////////////////////////////////////////////////////////////////


