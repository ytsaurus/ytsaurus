#pragma once

#include "public.h"

#include <yt/yt/core/misc/heap.h>

namespace NYT::NFlow {

template <class TKey, class TValue, class TComparator = std::less<TValue>>
class TKeyedHeap
{
public:
    TKeyedHeap(TComparator comparator = TComparator());

    size_t Size() const;
    bool Empty() const;

    template <class TInsertKey, class TInsertValue>
    void Set(TInsertKey&& key, TInsertValue&& value);

    template <class TEraseKey>
    bool Erase(const TEraseKey& key);

    const TValue& TopValue() const;
    const TKey& TopKey() const;

    void Clear();

private:
    using TStorageHashMap = THashMap<TKey, std::pair<TValue, size_t>>;
    using THeapItem = typename TStorageHashMap::iterator;

    TStorageHashMap HashMap_;
    std::vector<THeapItem> Heap_;
    TComparator Comparator_;

    void OnAssignHelper(size_t index);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define KEYED_HEAP_INL_H_
#include "keyed_heap-inl.h"
#undef KEYED_HEAP_INL_H_
