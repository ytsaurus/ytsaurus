#pragma once

#include "public.h"

#include <util/generic/hash.h>

#include <list>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// FIFO queue with random access. Elements are pairs of (key, value). All keys in queue are distinct.
// Push either adds new element to the end of the queue or updates existing one.
// Pop can either extract top element or an element from inside the queue accessed via key.
template <class TKey, class TValue>
class TRandomAccessQueue
{
public:
    using TEntry = std::pair<TKey, TValue>;

    TRandomAccessQueue() = default;

    //! Pushes new entry to the queue or updates existing one.
    void Push(TEntry&& entry);

    //! Returns first element in the queue.
    TEntry Pop();

    //! Retruns element with specified key in the queue or Null if none exists;
    TNullable<TEntry> Pop(const TKey& key);

    //! Returns number of elements in the queue.
    i64 Size() const;

    //! Returns |true| if queue has no elements.
    bool IsEmpty() const;

    //! Removes all elements from the queue.
    void Clear();

    template <class TSaveContext>
    void Save(TSaveContext& context) const;

    template <class TLoadContext>
    void Load(TLoadContext& context);

private:
    using TListIterator = typename std::list<TEntry>::iterator;

    std::list<TEntry> Queue_;
    THashMap<TKey, TListIterator> KeyToEntryMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANDOM_ACCESS_QUEUE_INL_H_
#include "random_access_queue-inl.h"
#undef RANDOM_ACCESS_QUEUE_INL_H_
