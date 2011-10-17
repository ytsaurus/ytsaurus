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
    /* Transitions
     *   Normal -> LoadingSnapshot,
     *   Normal -> SavingSnapshot,
     *   HasPendingChanges -> Normal
     * are performed from the user thread.
     *
     * Transitions
     *   LoadingSnapshot -> Normal,
     *   SavingSnapshot -> HasPendingChanges
     * are performed from the invoker.
     */
    DECLARE_ENUM(EState,
        (Normal)
        (LoadingSnapshot)
        (SavingSnapshot)
        (HasPendingChanges)
    );

    EState State;
};

//! Snapshottable map used to store various meta-state tables.
/*!
 * \tparam TKey Key type.
 * \tparam TValue Value type.
 * \tparam THash Hash function for keys.
 * 
 * \note All the public methods must be called from one thread
 * \note TValue must have the following methods:
 *          TAutoPtr<TValue> Clone();
 *          void Save(TOutputStream* output);
 *          static TAutoPtr<TValue> Load(TInputStream* input);
 */
template <
    class TKey,
    class TValue,
    class THash = ::THash<TKey>
>
class TMetaStateMap
    : protected TMetaStateMapBase
{
public:
    typedef TMetaStateMap<TKey, TValue, THash> TThis;
    typedef yhash_map<TKey, TValue*, THash> TMap;

    /*!
     * \note If TPatch::Second() == true then the value is to be updated,
     *       if TPatch::Second() == false then the value is to be removed.
     */
    typedef TPair<TValue*, bool> TPatch;
    typedef yhash_map<TKey, TPatch, THash> TPatchMap;

    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;

    ~TMetaStateMap()
    {
        switch (State)
        {
            case EState::LoadingSnapshot:
            case EState::SavingSnapshot:
                YUNREACHABLE();

            case EState::Normal:
            case EState::HasPendingChanges:
                FOREACH (const auto& pair, MainMap) {
                    delete pair.Second();
                }
                MainMap.clear();
                FOREACH (const auto& pair, PatchMap) {
                    if (pair.Second().Second()) {
                        delete pair.Second().First();
                    } else {
                        YASSERT(pair.Second().First() == NULL);
                    }
                }
                PatchMap.clear();
                break;
        }
    }

    //! Inserts a key-value pair.
    /*!
     * \note Value is owned by map after insertion.
     * \note Fails if the key is already in map.
     */
    void Insert(const TKey& key, TValue* value)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt == PatchMap.end()) {
                    YASSERT(MainMap.find(key) == MainMap.end());
                    YVERIFY(PatchMap.insert(MakePair(key, MakePair(value, true))).Second());
                } else {
                    YASSERT(!patchIt->Second().Second());
                    patchIt->Second() = MakePair(value, true);
                }
                break;
            }
            case EState::HasPendingChanges:
            case EState::Normal:
                MergeTempTablesIfNeeded();
                YVERIFY(MainMap.insert(MakePair(key, value)).Second());
                break;

            default:
                YUNREACHABLE();
        }
    }

    //! Tries to find a value by its key. The returned value is read-only.
    /*!
     * \param key A key.
     * \return Pointer to the const value if found, NULL otherwise.
     */
    const TValue* Find(const TKey& key) const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt != PatchMap.end()) {
                    return patchIt->Second().First();
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

        auto it = MainMap.find(key);
        return it == MainMap.end() ? NULL : it->Second();
    }

    //! Tries to find a value by its key. May return a modifiable copy if snapshot creation is in progress.
    /*!
     * \param key A key.
     * \return Pointer to the value if found, NULL otherwise.
     */
    TValue* FindForUpdate(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();
                auto mapIt = MainMap.find(key);
                return mapIt == MainMap.end() ? NULL : mapIt->Second();
            }
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                if (patchIt != PatchMap.end()) {
                    return patchIt->Second().First();
                }         

                auto mapIt = MainMap.find(key);
                if (mapIt == MainMap.end()) {
                    return NULL;
                }

                TAutoPtr<TValue> clonedValue = mapIt->Second()->Clone();
                auto patchPair = PatchMap.insert(
                    MakePair(key, MakePair(clonedValue.Release(), true)));
                return patchPair.First()->Second().First();
            }
            default:
                YUNREACHABLE();
        }
    }

    //! Returns a read-only value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns Const reference to the value.
     */
    const TValue& Get(const TKey& key) const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        auto value = Find(key);
        YASSERT(value != NULL);
        return *value;
    }

    //! Returns a modifiable value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns Reference to the value.
     */
    TValue& GetForUpdate(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        auto value = FindForUpdate(key);
        YASSERT(value != NULL);
        return *value;
    }

    //! Removes the key from the map and deletes the corresponding value.
    void Remove(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

         switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();

                auto it = MainMap.find(key);
                YASSERT(it != MainMap.end());
                delete it->Second();
                MainMap.erase(it);
                break;
            }
            case EState::SavingSnapshot: {
                auto patchIt = PatchMap.find(key);
                auto mainIt = MainMap.find(key);
                if (patchIt == PatchMap.end()) {
                    YASSERT(mainIt != MainMap.end());
                    YVERIFY(PatchMap.insert(MakePair(key, TPatch(NULL, false))).Second());
                } else {
                    YASSERT(patchIt->Second().Second());
                    delete patchIt->Second().First();
                    if (mainIt == MainMap.end()) {
                        PatchMap.erase(patchIt);
                    } else {
                        patchIt->Second() = TPatch(NULL, false);
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
     *  \param key A key to check.
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
                FOREACH(const auto& pair, MainMap) {
                    delete pair.Second();
                }
                MainMap.clear();
                break;
            }
            case EState::SavingSnapshot: {
                FOREACH (const auto& pair, PatchMap) {
                    if (pair.Second().Second()) {
                        delete pair.Second().First();
                    } else {
                        YASSERT(pair.Second().First() == NULL);
                    }
                }
                PatchMap.clear();

                FOREACH (const auto& pair, MainMap) {
                    PatchMap.insert(MakePair(pair.First(), TPatch(NULL, false)));
                }
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    //! (Unordered) begin()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TIterator Begin()
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
        MergeTempTablesIfNeeded();
        return MainMap.begin();
    }

    //! (Unordered) end()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TIterator End()
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
        MergeTempTablesIfNeeded();
        return MainMap.end();
    }
    
    //! (Unordered) const begin()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TConstIterator Begin() const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
        MergeTempTablesIfNeeded();
        return MainMap.begin();
    }

    //! (Unordered) const end()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TConstIterator End() const
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        YASSERT(State == EState::Normal || State == EState::HasPendingChanges);
        MergeTempTablesIfNeeded();
        return MainMap.end();
    }

    //! Asynchronously saves the map to the stream.
    /*!
     * This method saves the snapshot of the map as it seen at the moment of
     * invocation. All further updates are accepted but kept in-memory.
     * \param invoker Invoker for actual heavy work.
     * \param stream Output stream.
     * \return Callback on successful save.
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
        
        MainMap.clear();
        PatchMap.clear();
        State = EState::LoadingSnapshot;

        return
            FromMethod(&TMetaStateMap::DoLoad, this, input)
            ->AsyncVia(invoker)
            ->Do();
    }
    
private:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);
    
    bool IsSavingSnapshot;

    TMap MainMap;
    TPatchMap PatchMap;
    
    typedef TPair<TKey, TValue*> TItem;

    TVoid DoSave(TOutputStream* output)
    {
        ::Save(output, static_cast<i32>(MainMap.size()));

        yvector<TItem> items(MainMap.begin(), MainMap.end());
        Sort(items.begin(), items.end());

        FOREACH(const auto& item, items) {
            ::Save(output, item.First());
            item.Second()->Save(output);
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
            TValue* value = TValue::Load(input).Release();
            MainMap.insert(MakePair(key, value));
        }

        State = EState::Normal;

        return TVoid();
    }

    void MergeTempTablesIfNeeded()
    {
        if (State != EState::HasPendingChanges) return;

        FOREACH(const auto& pair, PatchMap) {
            auto& patch = pair.Second();
            auto mainIt = MainMap.find(pair.First());
            if (patch.Second()) {
                if (mainIt == MainMap.end()) {
                    YVERIFY(MainMap.insert(MakePair(pair.First(), patch.First())).Second());
                } else {
                    delete mainIt->Second();
                    mainIt->Second() = patch.First();
                }
            } else {
                YASSERT(mainIt != MainMap.end());
                delete mainIt->Second();
                MainMap.erase(mainIt);
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

template<class TKey, class TValue, class THash>
inline auto Begin(NMetaState::TMetaStateMap<TKey, TValue, THash>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

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

// TODO: drop this
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


