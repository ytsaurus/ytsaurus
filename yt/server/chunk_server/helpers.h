#pragma once

#include "public.h"
#include "chunk_tree_statistics.h"

#include <yt/server/cypress_server/public.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor);

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor);

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd);
void DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd);

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child);
void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);
void AppendChunkTreeChild(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics);
void AccumulateUniqueAncestorsStatistics(
    TChunkList* chunkList,
    const TChunkTreeStatistics& statisticsDelta);
void ResetChunkListStatistics(TChunkList* chunkList);
void RecomputeChunkListStatistics(TChunkList* chunkList);

std::vector<TChunkOwnerBase*> GetOwningNodes(
    TChunkTree* chunkTree);
TFuture<NYson::TYsonString> GetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTree* chunkTree);


bool IsEmpty(const TChunkList* chunkList);
bool IsEmpty(const TChunkTree* chunkTree);


bool IsEmpty(const TChunkList* chunkList);
bool IsEmpty(const TChunkTree* chunkTree);

NTableClient::TOwningKey GetMaxKey(const TChunk* chunk);
NTableClient::TOwningKey GetMaxKey(const TChunkTree* chunkTree);

NTableClient::TOwningKey GetMinKey(const TChunk* chunk);
NTableClient::TOwningKey GetMinKey(const TChunkTree* chunkTree);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
