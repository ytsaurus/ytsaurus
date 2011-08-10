#pragma once

#include "common.h"
#include "../misc/assert.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: What are the guarantees?
// Shall forUpdate alter the behaviour of Find and Get to wait until snapshot would be created?
// Shall Load/Save do all work via passed invoker?
// Shall Load guarantee that map swap will be atomic?

//! Snapshotable map which keeps values by pointers.
/*
 * \tparam TKey Type of the key.
 * \tparam TValue Type of the value.
 * \tparam THash Hash function to be used.
 */
template <class TKey, class TValue, class THash = ::THash<TKey> >
class TMetaStateRefMap
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef yhash_map<TKey, TValuePtr, THash> TMap;
    typedef typename TMap::iterator TIterator;

    //! Inserts or updates a key-value pair.
    /*! \returns True iff the key is new.
     */
    bool Insert(const TKey& key, TValuePtr value)
    {
        return Map.insert(MakePair(key, value)).Second();
    }

    //! Tries to find the key in the map.
    /*!
     * \param key The key.
     * \param forUpdate Hint whether the value will be altered.
     * \return Pointer to the value if the key exists in the map, null otherwise.
     */
    TValuePtr Find(const TKey& key, bool forUpdate = false)
    {
        UNUSED(forUpdate);
        typename TMap::iterator it = Map.find(key);
        if (it == Map.end())
            return NULL;
        else
            return it->Second();
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
        UNUSED(forUpdate);
        TValuePtr value = Find(key);
        YASSERT(~value != NULL);
        return value;
    }

    //! Removes the key from the map.
    bool Remove(const TKey& key)
    {
        return Map.erase(key) == 1;
    }

    //! Checks whether the key exists in the map.
    bool Contains(const TKey& key) const
    {
        return Map.find(key) != Map.end();
    }

    //! Removes all keys from the map (hence effectively dropping smart pointers
    //! to the corresponding values).
    void Clear()
    {
        Map.clear();
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
        // TODO: implement
        UNUSED(invoker);
        UNUSED(stream);
        return new TAsyncResult<TVoid>(TVoid());
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
        // TODO: implement
        UNUSED(invoker);
        UNUSED(stream);
        Map.clear();
        return new TAsyncResult<TVoid>(TVoid());
    }
    
private:
    TMap Map;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

