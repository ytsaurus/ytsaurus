#pragma once

#include "public.h"

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBipartiteMapElementType,
    (First)
    (Second)
);

////////////////////////////////////////////////////////////////////////////////

template <class TElement, class TData>
class TBipartiteMap
{
public:
    using TPair = std::pair<TElement, TElement>;

    size_t Size() const;
    bool Empty() const;

    void AddPair(TPair pair, TData data);
    void RemoveElement(TElement element, EBipartiteMapElementType type);

    TData& operator[](const TPair& pair);
    const TData& operator[](const TPair& pair) const;

private:
    using TElementNeighbourIndex = THashMap<TElement, THashSet<TElement>>;

    THashMap<TPair, TData> Storage_;
    TEnumIndexedArray<EBipartiteMapElementType, TElementNeighbourIndex> ElementIndex_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define BIPARTITE_MAP_INL_H_
#include "bipartite_map-inl.h"
#undef BIPARTITE_MAP_INL_H_
