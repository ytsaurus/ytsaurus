#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <server/cypress_server/public.h>

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
    F chunkAction,
    bool resetSorted = true);

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children,
    bool resetSorted = true);

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child,
    bool resetSorted = true);

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);

void SerializeOwningNodesPaths(
    NCypressServer::TCypressManagerPtr cypressManager,
    TChunkTree* chunkTree,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
