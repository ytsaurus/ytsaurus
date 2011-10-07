#pragma once

#include "common.h"
#include "../misc/enum.h"
#include "../misc/assert.h"
#include "../misc/foreach.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// TODO: DECLARE_ENUM cannot be used in a template class.
class TMetaStateMapBase
    : private TNonCopyable
{
protected:
    DECLARE_ENUM(EState,
        (Normal)
        (SavingSnapshot)
        (SavedSnapshot)
    );

    EState State;
};

//! Snapshottable map used to store various meta-state tables.
/*!
 * \tparam TKey Key type.
 * \tparam TValue Value type.
 * \tparam THash Hash function for keys.
 */
template <
    class TKey,
    class TValue,
    class THash = ::THash<TKey>
>
class TMetaStateMap
    : public TMetaStateMapBase
{
public:
    typedef yhash_map<TKey, TValue*, THash> TMap;
    typedef yhash_set<TKey, THash> TKeySet;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;

    ~TMetaStateMap()
    {
        FOREACH (const auto& pair, Map) {
            delete pair.Second();
        }
        Map.clear();

        FOREACH (const auto& pair, InsertionMap) {
            delete pair.Second();
        }
        InsertionMap.clear();
    }

    //! Inserts a key-value pair.
    /*!
     *  Does nothing if the key is already in map.
     *  \returns True iff the key is new.
     */
    bool Insert(const TKey& key, TValue* value)
    {
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }

        if (State == EState::Normal) {
            return Map.insert(MakePair(key, value)).Second();
        }

        bool isAlreadyDeleted = DeletionSet.erase(key) == 1;
        if (isAlreadyDeleted || Map.find(key) == Map.end()) {
            return InsertionMap.insert(MakePair(key, value)).Second();
        } else {
            return false;
        }

    }

    //! Tries to find a value by its key. The returned value is read-only.
    /*!
     * \param key A key.
     * \return Pointer to the const value if found, NULL otherwise.
     */
    const TValue* Find(const TKey& key) const
    {
        if (State != EState::Normal) {
            const auto insertionIt = InsertionMap.find(key);
            if (insertionIt != InsertionMap.end()) {
                YASSERT(DeletionSet.find(key) == DeletionSet.end());
                return insertionIt->Second();
            }

            const auto deletionIt = DeletionSet.find(key);
            if (deletionIt != DeletionSet.end()) {
                return NULL;
            }
        }

        const auto it = Map.find(key);
        return it == Map.end() ? NULL : it->Second();
    }

    //! Tries to find a value by its key. May return a modifiable copy if snapshot creation is in progress.
    /*!
     * \param key A key.
     * \return Pointer to the value if found,  otherwise.
     */
    TValue* FindForUpdate(const TKey& key)
    {
        if (State != EState::Normal) {
            auto insertionIt = InsertionMap.find(key);
            if (insertionIt != InsertionMap.end()) {
                YASSERT(DeletionSet.find(key) == DeletionSet.end());
                return insertionIt->Second();
            }

            auto deletionIt = DeletionSet.find(key);
            if (deletionIt != DeletionSet.end()) {
                return NULL;
            }
        }

        auto mapIt = Map.find(key);
        if (mapIt == Map.end()) {
            return NULL;
        }

        TValue* value = mapIt->Second();
        if (State != EState::SavingSnapshot) {
            return value;
        }

        TAutoPtr<TValue> clonedValue = value->Clone();
        auto insertionPair = InsertionMap.insert(MakePair(key, clonedValue.Release()));
        YASSERT(insertionPair.Second());
        return insertionPair.First()->Second();
    }

    //! Returns a read-only value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns Const reference to the value.
     */
    const TValue& Get(const TKey& key) const
    {
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
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }

        if (State == EState::Normal) {
            auto it = Map.find(key);
            if (it == Map.end()) {
                return false;
            }
            delete it->Second();
            Map.erase(it);
            return true;
        } else {
            bool wasInInsertion = InsertionMap.erase(key) == 1;
            if (Map.find(key) != Map.end()) {
                bool wasInDeletion = !DeletionSet.insert(key).Second();
                YASSERT(!wasInInsertion || !wasInDeletion);
                return !wasInDeletion;
            }
            return wasInInsertion;
        }
    }

    //! Checks whether the key exists in the map.
    /*!
     *  \param key A key to check.
     *  \return True iff the key exists in the map.
     */
    bool Contains(const TKey& key) const
    {
        return Find(key) != NULL;
    }

    //! Clears the map.
    void Clear()
    {
        if (State == EState::Normal) {
            FOREACH(const auto& pair, Map) {
                delete pair.Second();
            }
            Map.clear();
        } else {
            FOREACH (const auto& pair, InsertionMap) {
                delete pair.Second();
            }
            InsertionMap.clear();

            FOREACH(const auto& pair, Map) {
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
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        return Map.begin();
    }

    //! (Unordered) end()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TIterator End()
    {
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        return Map.end();
    }

    //! (Unordered) const begin()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TConstIterator Begin() const
    {
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        return Map.begin();
    }

    //! (Unordered) const end()-iterator.
    /*!
     *  Iteration is only possible when no snapshot is being created.
     */
    TConstIterator End() const
    {
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        return Map.end();
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
        TOutputStream* stream)
    {
        YASSERT(~invoker != NULL);
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        MaybeMergeTempTables();

        YASSERT(InsertionMap.empty());
        YASSERT(DeletionSet.empty());
        State = EState::SavingSnapshot;
        return
            FromMethod(&TMetaStateMap::DoSave, this, stream)
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
        TInputStream* stream)
    {
        YASSERT(~invoker != NULL);
        YASSERT(State == EState::Normal || State == EState::SavedSnapshot);
        MaybeMergeTempTables();

        YASSERT(InsertionMap.empty());
        YASSERT(DeletionSet.empty());
        Map.clear();
        return
            FromMethod(&TMetaStateMap::DoLoad, this, stream)
            ->AsyncVia(invoker)
            ->Do();
    }
    
private:
    TMap Map;

    // Each key couldn't be both in InsertionMap and DeletionSet
    TMap InsertionMap;
    TKeySet DeletionSet;

    typedef TPair<TKey, TValue> TItem;
    static bool ItemComparer(const TItem& i1, const TItem& i2)
    {
        return i1.first < i2.first;
    }

    TVoid DoSave(TOutputStream* stream)
    {
        UNUSED(stream);
        YASSERT(false);
        // TODO: implement
        /*
        *stream << static_cast<i64>(Map.size());

        yvector<TItem> items(Map.begin(), Map.end());
        std::sort(items.begin(), items.end(), ItemComparer);

        FOREACH(const auto& item, items) {
            *stream << it->first << it->second;
        }
        */
        State = EState::SavedSnapshot;
        return TVoid();
    }

    TVoid DoLoad(TInputStream* stream)
    {
        UNUSED(stream);
        YASSERT(false);
        // TODO: implement
        /*
        i64 size;
        *stream >> size;

        YASSERT(size >= 0);

        for (i64 index = 0; index < size; ++index) {
            TKey key;
            TValue value;
            *stream >> key >> value;
            Map.insert(MakePair(key, value));
        }
        */
        return TVoid();
    }

    void MaybeMergeTempTables()
    {
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }
    }

    void MergeTempTables()
    {
        YASSERT(State == EState::SavedSnapshot);

        FOREACH(const auto& pair, InsertionMap) {
            YASSERT(DeletionSet.find(pair.first) == DeletionSet.end());
            auto it = Map.find(pair.First());
            if (it != Map.end()) {
                delete it->Second();
            }
            Map[pair.first] = pair.second;
        }

        FOREACH(const auto& key, DeletionSet) {
            YASSERT(InsertionMap.find(key) == InsertionMap.end());
            auto it = Map.find(key);
            YASSERT(it != Map.end());
            delete it->Second();
            Map.erase(it);
        }

        InsertionMap.clear();
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
    entityType& Get ## entityName ## ForUpdate(const idType& id)

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


