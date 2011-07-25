#pragma once

#include "common.h"
#include "../misc/assert.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: document and implement
template <class TKey, class TValue, class THash = ::THash<TKey> >
class TMetaStateRefMap
{
public:
    typedef TIntrusivePtr<TValue> TValuePtr;
    typedef yhash_map<TKey, TValuePtr, THash> TMap;
    typedef typename TMap::iterator TIterator;

    bool Insert(const TKey& key, TValuePtr value)
    {
        return Map.insert(MakePair(key, value)).Second();
    }

    TValuePtr Find(const TKey& key, bool forUpdate = false)
    {
        UNUSED(forUpdate);
        typename TMap::iterator it = Map.find(key);
        if (it == Map.end())
            return NULL;
        else
            return it->Second();
    }

    TValuePtr Get(const TKey& key, bool forUpdate = false)
    {
        UNUSED(forUpdate);
        TValuePtr value = Find(key);
        YASSERT(~value != NULL);
        return value;
    }

    bool Remove(const TKey& key)
    {
        return Map.erase(key) == 1;
    }

    bool Contains(const TKey& key) const
    {
        return Map.find(key) != Map.end();
    }

    void Clear()
    {
        Map.clear();
    }

    TIterator Begin()
    {
        return Map.begin();
    }

    TIterator End()
    {
        return Map.end();
    }

    TAsyncResult<TVoid>::TPtr Save(
        IInvoker::TPtr invoker,
        TOutputStream& stream)
    {
        // TODO: implement
        UNUSED(invoker);
        UNUSED(stream);
        return new TAsyncResult<TVoid>(TVoid());
    }

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
