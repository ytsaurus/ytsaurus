#pragma once

#include "common.h"
#include "../misc/enum.h"
#include "../misc/assert.h"
#include "../misc/foreach.h"
#include "../misc/serialize.h"

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
     * are performed from user thread.
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
    typedef yhash_set<TKey, THash> TKeySet;
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
                FOREACH (const auto& pair, UpdateMap) {
                    delete pair.Second();
                }
                UpdateMap.clear();
                DeletionSet.clear();
                break;
        }
    }

    //! Inserts a key-value pair.
    /*!
     * \returns True iff the key is new.
     * 
     * \note Does nothing if the key is already in map.
     */
    bool Insert(const TKey& key, TValue* value)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

        switch (State) {
            case EState::SavingSnapshot:
                if (DeletionSet.erase(key) > 0) {
                    YVERIFY(UpdateMap.insert(MakePair(key, value)).Second());
                    return true;
                }

                if (MainMap.find(key) != MainMap.end()) {
                    return false;
                }

                return UpdateMap.insert(MakePair(key, value)).Second();
            
            case EState::HasPendingChanges:
            case EState::Normal:
                MergeTempTablesIfNeeded();
                return MainMap.insert(MakePair(key, value)).Second();

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
                auto insertionIt = UpdateMap.find(key);
                if (insertionIt != UpdateMap.end()) {
                    YASSERT(DeletionSet.find(key) == DeletionSet.end());
                    return insertionIt->Second();
                }

                auto deletionIt = DeletionSet.find(key);
                if (deletionIt != DeletionSet.end()) {
                    return NULL;
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
     * \return Pointer to the value if found,  otherwise.
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
                auto insertionIt = UpdateMap.find(key);
                if (insertionIt != UpdateMap.end()) {
                    YASSERT(DeletionSet.find(key) == DeletionSet.end());
                    return insertionIt->Second();
                }

                if (DeletionSet.find(key) != DeletionSet.end()) {
                    return NULL;
                }              

                auto mapIt = MainMap.find(key);
                if (mapIt == MainMap.end()) {
                    return NULL;
                }

                TAutoPtr<TValue> clonedValue = mapIt->Second()->Clone();
                auto insertionPair = UpdateMap.insert(MakePair(key, clonedValue.Release()));
                YASSERT(insertionPair.Second());
                return insertionPair.First()->Second();
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

    //! Removes the key from the map.
    /*!
     *  \returns True iff the key was in the map.
     */
    bool Remove(const TKey& key)
    {
        VERIFY_THREAD_AFFINITY(UserThread);

         switch (State) {
            case EState::HasPendingChanges:
            case EState::Normal: {
                MergeTempTablesIfNeeded();

                auto it = MainMap.find(key);
                if (it == MainMap.end()) {
                    return false;
                }

                delete it->Second();
                MainMap.erase(it);
                return true;
            }
            case EState::SavingSnapshot:
                if (MainMap.find(key) != MainMap.end()) {
                    UpdateMap.erase(key);
                    return DeletionSet.insert(key).Second();
                }

                return UpdateMap.erase(key) > 0;

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

        if (State == EState::Normal) {
            FOREACH(const auto& pair, MainMap) {
                delete pair.Second();
            }
            MainMap.clear();
        } else {
            FOREACH (const auto& pair, UpdateMap) {
                delete pair.Second();
            }
            UpdateMap.clear();

            FOREACH(const auto& pair, MainMap) {
                DeletionSet.insert(pair.first);
            }
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

        YASSERT(UpdateMap.empty());
        YASSERT(DeletionSet.empty());
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
        UpdateMap.clear();
        DeletionSet.clear();
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

    // Each key couldn't be both in UpdateMap and DeletionSet
    TMap UpdateMap;
    TKeySet DeletionSet;
    
    typedef TPair<TKey, TValue*> TItem;
    
    // Default comparer requires TValue to be comparable, we don't need it.
    static bool ItemComparer(const TItem& i1, const TItem& i2) 
    {
        return i1.first < i2.first;
    }

    TVoid DoSave(TOutputStream* output)
    {
        *output << static_cast<i32>(MainMap.size());

        yvector<TItem> items(MainMap.begin(), MainMap.end());
        std::sort(items.begin(), items.end(), ItemComparer);

        FOREACH(const auto& item, items) {
            Write(*output, item.First());
            item.Second()->Save(output);
        }
        
        State = EState::HasPendingChanges;

        return TVoid();
    }

    TVoid DoLoad(TInputStream* input)
    {
        i32 size;
        *input >> size;

        YASSERT(size >= 0);

        for (i32 index = 0; index < size; ++index) {
            TKey key;
            Read(*input, &key);
            TValue* value = TValue::Load(input).Release();
            MainMap.insert(MakePair(key, value));
        }

        State = EState::Normal;

        return TVoid();
    }

    void MergeTempTablesIfNeeded()
    {
        if (State != EState::HasPendingChanges) return;

        FOREACH(const auto& pair, UpdateMap) {
            YASSERT(DeletionSet.find(pair.first) == DeletionSet.end());
            auto it = MainMap.find(pair.First());
            if (it != MainMap.end()) {
                delete it->Second();
                it->Second() = pair.Second();
            } else {
                MainMap.insert(pair);
            }
        }

        FOREACH(const auto& key, DeletionSet) {
            YASSERT(UpdateMap.find(key) == UpdateMap.end());
            auto it = MainMap.find(key);
            YASSERT(it != MainMap.end());
            delete it->Second();
            MainMap.erase(it);
        }

        UpdateMap.clear();
        DeletionSet.clear();

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


