#pragma once

#include "common.h"
#include "../misc/enum.h"
#include "../misc/assert.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: What are the guarantees?
// Shall forUpdate alter the behaviour of Find and Get to wait until snapshot would be created?
// NO, it should return a copy
// Shall Load/Save do all work via passed invoker?
// YES
// Shall Load guarantee that map swap will be atomic?
// NO, there will be no concurrent access at this time

// Guarantees:
// All public functions will be called from one thread

// hack to use smart enums
class TMetaStateRefMapBase
{
protected:
    DECLARE_ENUM(EState,
        (Normal)
        (SavingSnapshot)
        (SavedSnapshot)
    );
    EState State;
};

//! Snapshotable map which keeps values by pointers.
/*
 * \tparam TKey Type of the key.
 * \tparam TValue Type of the value.
 * \tparam THash Hash function to be used.
 */
template <class TKey, class TValue, class THash = ::THash<TKey> >
class TMetaStateRefMap
    : public TMetaStateRefMapBase
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef yhash_map<TKey, TValuePtr, THash> TMap;
    typedef yhash_set<TKey, THash> TSet;
    typedef typename TMap::iterator TIterator;

    //! Inserts a key-value pair.
    /*!
     * Does nothing if the key is already in map
     * \returns True iff the key is new.
     */
    bool Insert(const TKey& key, TValuePtr value)
    {
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }
        if (State == EState::Normal) {
            return Map.insert(MakePair(key, value)).second;
        }
        bool isAlreadyDeleted = (DeletionsSet.erase(key) == 1);
        if (isAlreadyDeleted || Map.find(key) == Map.end()) {
            return InsertsMap.insert(MakePair(key, value)).second;
        }
        return false;
    }

    //! Tries to find the key in the map.
    /*!
     * \param key The key.
     * \param forUpdate Hint whether the value will be altered.
     * \return Pointer to the value if the key exists in the map, null otherwise.
     */
    TValuePtr Find(const TKey& key, bool forUpdate = false)
    {
        if (State != EState::Normal) {
            typename TMap::iterator insertsIt = InsertsMap.find(key);
            if (insertsIt != InsertsMap.end()) {
                return insertsIt->second;
            }
            typename TSet::iterator deletionsIt = DeletionsSet.find(key);
            if (deletionsIt != DeletionsSet.end()) {
                return NULL;
            }
        }
        typename TMap::iterator it = Map.find(key);
        if (it == Map.end()) {
            return NULL;
        }
        if (forUpdate) {
            TValuePtr newValue = new TValue(*it->second);
            YVERIFY(InsertsMap.insert(MakePair(key, newValue)).second);
            return newValue;
        }
        return it->second;
    }

    //! Returns the value corresponding to the key.
    /*!
     * In contrast to #Find this method fails if the key does not exist in the
     * map.
     * \param key The key.
     * \param forUpdate Hint whether the value will be altered.
     * \returns Pointer to the value.
     */
    TValuePtr Get(const TKey& key, bool forUpdate = false)
    {
        TValuePtr value = Find(key, forUpdate);
        YASSERT(~value != NULL);
        return value;
    }

    //! Removes the key from the map.
    /*! \returns True iff the key was in map
     */
    bool Remove(const TKey& key)
    {
        if (State == EState::SavedSnapshot) {
            MergeTempTables();
        }
        if (State == EState::Normal) {
            return Map.erase(key) == 1;
        }
        bool wasInInserts = (InsertsMap.erase(key) == 1);
        if (Map.find(key) != Map.end()) {
            DeletionsSet.insert(key).second;
            return true;
        }
        return wasInInserts;
    }

    //! Checks whether the key exists in the map.
    bool Contains(const TKey& key)
    {
        return (~Find(key) != NULL);
    }

    //! Removes all keys from the map (hence effectively dropping smart pointers
    //! to the corresponding values).
    void Clear()
    {
        if (State == EState::Normal) {
            Map.clear();
            return;
        }

        InsertsMap.clear();
        for (TIterator it = Map.begin(); it != Map.end(); ++it) {
            const TKey& key = it->first;
            DeletionsSet.insert(key);
        }
    }

    //! (Unordered) begin()-iterator.
    TIterator Begin()
    {
        return Map.begin();
    }

    //! (Unordered) end()-iterator.
    TIterator End()
    {
        return Map.end();
    }

    //! Asynchronously saves the map to the stream.
    /*
     * This method saves the snapshot of the map as it seen at the moment of
     * invokation. All further updates are accepted but kept in-memory.
     * \param invoker Invoker for actual heavy work.
     * \param stream Output stream.
     * \return Callback on successful save.
     */
    TAsyncResult<TVoid>::TPtr Save(
        IInvoker::TPtr invoker,
        TOutputStream& stream)
    {
        YASSERT(State == EState::Normal);
        YASSERT(InsertsMap.size() == 0);
        YASSERT(DeletionsSet.size() == 0);
        State = EState::SavingSnapshot;
        return
            FromMethod(&TMetaStateRefMap::DoSave, this, stream)
            ->AsyncVia(invoker)
            ->Do();
    }

    //! Asynchronously loads the map from the stream.
    /*
     * This method loads the snapshot of the map in the background and at some
     * moment in the future swaps current map with the loaded one.
     * \param invoker Invoker for actual heavy work.
     * \param stream Input stream.
     * \return Callback on successful load.
     */
    TAsyncResult<TVoid>::TPtr Load(
        IInvoker::TPtr invoker,
        TInputStream& stream)
    {
        YASSERT(State == EState::Normal);
        YASSERT(InsertsMap.size() == 0);
        YASSERT(DeletionsSet.size() == 0);
        Map.clear();
        return
            FromMethod(&TMetaStateRefMap::DoLoad, this, stream)
            ->AsyncVia(invoker)
            ->Do();
    }
    
private:
    TMap Map;
    TMap InsertsMap;
    TSet DeletionsSet;

    typedef TPair<TKey, TValuePtr> TItem;
    static bool ItemComparer(const TItem& i1, const TItem& i2)
    {
        return (i1.first < i2.first);
    }

    TVoid DoSave(TOutputStream& stream)
    {
        stream << Map.size();
        yvector<TItem> items(Map.begin(), Map.end());
        std::sort(items.begin(), items.end(), ItemComparer);
        for (typename yvector<TItem>::iterator it = items.begin();
            it != items.end();
            ++it) {
            //TODO: fix this when operator << is implemented
            //stream << it->first << *it->second;
        }
        State = EState::SavedSnapshot;
        return TVoid();
    }

    TVoid DoLoad(TInputStream& stream)
    {
        size_t size;
        stream >> size;
        for (size_t i = 0; i < size; ++i) {
            TKey key;
            TValue value;
            //TODO: fix this when operator >> is implemented
            //stream >> key >> value;
            Map.insert(MakePair(key, TValuePtr(&value)));
        }
        return TVoid();
    }

    void MergeTempTables()
    {
        for (TIterator it = InsertsMap.begin(); it != InsertsMap.end(); ++it) {
            Map[it->first] = it->second;
        }
        InsertsMap.clear();

        for (typename TSet::iterator it = DeletionsSet.begin();
            it != DeletionsSet.end();
            ++it)
        {
            Map.erase(*it);
        }
        DeletionsSet.clear();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

