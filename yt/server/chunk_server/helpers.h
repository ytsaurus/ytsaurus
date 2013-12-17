#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor);

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor);

template <class F>
void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    F chunkAction);

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children);

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child);

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
