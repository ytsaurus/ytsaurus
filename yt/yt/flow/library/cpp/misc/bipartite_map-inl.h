#pragma once

#ifndef BIPARTITE_MAP_INL_H_
    #error "Direct inclusion of this file is not allowed, include bipartite_map.h"
    // For the sake of sane code completion.
    #include "bipartite_map.h"
#endif

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TElement, class TData>
size_t TBipartiteMap<TElement, TData>::Size() const
{
    return Storage_.size();
}

template <class TElement, class TData>
bool TBipartiteMap<TElement, TData>::Empty() const
{
    return Storage_.empty();
}

template <class TElement, class TData>
void TBipartiteMap<TElement, TData>::AddPair(TPair pair, TData data)
{
    auto [_, inserted] = Storage_.emplace(pair, std::move(data));

    if (!inserted) {
        return;
    }

    auto& firstIndex = ElementIndex_[EBipartiteMapElementType::First][pair.first];
    auto& secondIndex = ElementIndex_[EBipartiteMapElementType::Second][pair.second];

    InsertOrCrash(firstIndex, std::move(pair.second));
    InsertOrCrash(secondIndex, std::move(pair.first));
}

template <class TElement, class TData>
void TBipartiteMap<TElement, TData>::RemoveElement(TElement element, EBipartiteMapElementType type)
{
    auto neighbourType = type == EBipartiteMapElementType::First ? EBipartiteMapElementType::Second : EBipartiteMapElementType::First;

    for (const auto& neighbour : ElementIndex_[type][element]) {
        auto pair = type == EBipartiteMapElementType::First ? TPair{element, neighbour} : TPair{neighbour, element};

        EraseOrCrash(Storage_, pair);
        EraseOrCrash(ElementIndex_[neighbourType][neighbour], element);
    }

    EraseOrCrash(ElementIndex_[type], element);
}

template <class TElement, class TData>
TData& TBipartiteMap<TElement, TData>::operator[](const TPair& pair)
{
    return Storage_[pair];
}

template <class TElement, class TData>
const TData& TBipartiteMap<TElement, TData>::operator[](const TPair& pair) const
{
    return Storage_[pair];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
