#pragma once

#include "common.h"
#include "../misc/enum.h"
#include "../misc/assert.h"
#include "../misc/foreach.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
struct TMetaStateMapValueTraits
{
    typedef TValue TStoredValue;

    typedef TValue& TValueRef;
    typedef const TValue& TConstValueRef;

    typedef TValue* TValuePtr;
    typedef const TValue* TConstValuePtr;

    static void Destroy(const TStoredValue& value)
    {
        UNUSED(value);
    }

    static TStoredValue Clone(const TStoredValue& value)
    {
        return value;
    }

    static TValuePtr ToPtr(TStoredValue& value)
    {
        return &value;
    }

    static TConstValuePtr ToPtr(const TStoredValue& value)
    {
        return &value;
    }

    static TValueRef ToRef(TValuePtr value)
    {
        return *value;
    }

    static TConstValueRef ToRef(TConstValuePtr value)
    {
        return *value;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
struct TMetaStateMapPtrTraits
{
    typedef TIntrusivePtr<TValue> TStoredValue;

    typedef TValue& TValueRef;
    typedef const TValue& TConstValueRef;

    typedef TValue* TValuePtr;
    typedef const TValue* TConstValuePtr;

    static void Destroy(const TStoredValue& value)
    {
        UNUSED(value);
    }

    static TStoredValue Clone(const TStoredValue& value)
    {
        UNUSED(value);
        YASSERT(false);
        return NULL;
    }

    static TValuePtr ToPtr(TStoredValue& value)
    {
        return ~value;
    }

    static TConstValuePtr ToPtr(const TStoredValue& value)
    {
        return ~value;
    }

    static TValueRef ToRef(TValuePtr value)
    {
        return *value;
    }

    static TConstValueRef ToRef(TConstValuePtr value)
    {
        return *value;
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: get rid of TODOS :)
// 
// TODO: What are the guarantees?
// Shall forUpdate alter the behavior of Find and Get to wait until snapshot would be created?
// NO, it should return a copy
// Shall Load/Save do all work via passed invoker?
// YES
// Shall Load guarantee that map swap will be atomic?
// NO, there will be no concurrent access at this time

// Guarantees:
// All public functions will be called from one thread

// Hack to use smart enums.
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

//! Snapshotable map used to store various meta-state tables.
/*!
 * \tparam TKey Key type.
 * \tparam TValue Value type.
 * \tparam THash Hash function for keys.
 */
template <
    class TKey,
    class TValue,
    class TTraits = TMetaStateMapValueTraits<TValue>,
    class THash = ::THash<TKey>
>
class TMetaStateMap
    : public TMetaStateMapBase
{
public:
    typedef yhash_map<TKey, typename TTraits::TStoredValue, THash> TMap;
    typedef yhash_set<TKey, THash> TKeySet;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;

    //! Inserts a key-value pair.
    /*!
     * Does nothing if the key is already in map
     * \returns True iff the key is new.
     */
    bool Insert(const TKey& key, typename TTraits::TConstValueRef value)
    {
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }
        if (State == EState::Normal) {
            return Map.insert(MakePair(key, value)).second;
        }
        bool isAlreadyDeleted = (DeletionSet.erase(key) == 1);
        if (isAlreadyDeleted || Map.find(key) == Map.end()) {
            return InsertionMap.insert(MakePair(key, value)).second;
        }
        return false;
    }

    //! Tries to find a value by its key. The returned value is read-only.
    /*!
     * \param key A key.
     * \return Pointer to the const value if found, NULL otherwise.
     */
    typename TTraits::TConstValuePtr Find(const TKey& key) const
    {
        if (State != EState::Normal) {
            const auto insertionIt = InsertionMap.find(key);
            if (insertionIt != InsertionMap.end()) {
                YASSERT(DeletionSet.find(key) == DeletionSet.end());
                return TTraits::ToPtr(insertionIt->second);
            }

            const auto deletionIt = DeletionSet.find(key);
            if (deletionIt != DeletionSet.end()) {
                return NULL;
            }
        }

        const auto it = Map.find(key);
        return it == Map.end() ? NULL : TTraits::ToPtr(it->Second());
    }

    //! Tries to find a value by its key. May return a modifiable copy if snapshot creation is in progress.
    /*!
     * \param key A key.
     * \return Pointer to the value if found,  otherwise.
     */
    typename TTraits::TValuePtr FindForUpdate(const TKey& key)
    {
        if (State != EState::Normal) {
            auto insertionIt = InsertionMap.find(key);
            if (insertionIt != InsertionMap.end()) {
                YASSERT(DeletionSet.find(key) == DeletionSet.end());
                return TTraits::ToPtr(insertionIt->second);
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

        typename TTraits::TStoredValue& value = mapIt->Second();
        if (State != EState::SavingSnapshot) {
            return TTraits::ToPtr(value);
        }

        auto clonedValue = TTraits::Clone(value);
        auto insertionPair = InsertionMap.insert(MakePair(key, clonedValue));
        YASSERT(insertionPair.second);
        return TTraits::ToPtr(insertionPair.First()->Second());
    }

    //! Returns a read-only value corresponding to the key.
    /*!
     * In contrast to #Find this method fails if the key does not exist in the map.
     * \param key A key.
     * \returns Const reference to the value.
     */
    typename TTraits::TConstValueRef Get(const TKey& key) const
    {
        auto value = Find(key);
        YASSERT(value != NULL);
        return TTraits::ToRef(value);
    }

    //! Returns a modifiable value corresponding to the key.
    /*!
     * In contrast to #Find this method fails if the key does not exist in the map.
     * \param key A key.
     * \returns Reference to the value.
     */
    typename TTraits::TValueRef GetForUpdate(const TKey& key)
    {
        auto value = FindForUpdate(key);
        YASSERT(value != NULL);
        return TTraits::ToRef(value);
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
            TTraits::Destroy(it->Second());
            Map.erase(it);
            return true;
        } else {
            bool wasInInserts = (InsertionMap.erase(key) == 1);
            if (Map.find(key) != Map.end()) {
                bool wasInDeletions = (DeletionSet.insert(key).Second() == false);
                YASSERT((wasInInserts && wasInDeletions) == false);
                return (!wasInDeletions);
            }
            return wasInInserts;
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
                TTraits::Destroy(pair.Second());
            }
            Map.clear();
            return;
        }

        InsertionMap.clear();
        FOREACH(const auto& pair, Map) {
            DeletionSet.insert(pair.first);
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

        YASSERT(InsertionMap.size() == 0);
        YASSERT(DeletionSet.size() == 0);
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
        // TODO: use traits
        YASSERT(State == EState::SavedSnapshot);

        FOREACH(const auto& pair, InsertionMap) {
            YASSERT(DeletionSet.find(pair.first) == DeletionSet.end());
            Map[pair.first] = pair.second;
        }

        FOREACH(TKey key, DeletionSet) {
            YASSERT(InsertionMap.find(key) == InsertionMap.end());
            Map.erase(key);
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

template<class TKey, class TValue, class TTraits, class THash>
inline auto Begin(NMetaState::TMetaStateMap<TKey, TValue, TTraits, THash>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

template<class TKey, class TValue, class TTraits, class THash>
inline auto End(NMetaState::TMetaStateMap<TKey, TValue, TTraits, THash>& collection) -> decltype(collection.End())
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


