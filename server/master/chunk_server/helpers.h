#pragma once

#include "public.h"
#include "chunk_tree_statistics.h"

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/security_server/cluster_resources.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Calls |functor(chunkList, child)| and |functor(parent(x), x)|, where |x|
//! iterates through proper ancestors of |chunkList|.
template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor, TChunkTree* child = nullptr);

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor);

TChunkList* GetUniqueParent(const TChunkTree* chunkTree);
int GetParentCount(const TChunkTree* chunkTree);
bool HasParent(const TChunkTree* chunkTree, TChunkList* potentialParent);

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd);
void DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd);

//! Set |childIndex|-th child of |chunkList| to |newChild|. It is up to caller
//! to deal with statistics.
void ReplaceChunkListChild(TChunkList* chunkList, int childIndex, TChunkTree* newChild);

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child);
void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);
void AppendChunkTreeChild(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics);

//! Apply statisticsDelta to all proper ancestors of |child|.
//! Both statistics and cumulative statistics are updated.
//! |statisticsDelta| should have |child|'s rank.
void AccumulateUniqueAncestorsStatistics(
    TChunkTree* child,
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

//! Returns the upper boundary key of a chunk. Throws if the chunk contains no
//! boundary info (i.e. it's not sorted).
NTableClient::TOwningKey GetUpperBoundKeyOrThrow(const TChunk* chunk);

//! Returns the upper boundary key of a chunk tree. Throws if the tree is empty
//! or the last chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TOwningKey GetUpperBoundKeyOrThrow(const TChunkTree* chunkTree);

//! Returns the minimum key of a chunk. Throws if the chunk contains no boundary
//! info (i.e. it's not sorted).
NTableClient::TOwningKey GetMinKeyOrThrow(const TChunk* chunk);

//! Returns the minimum key of a chunk tree. Throws if the tree is empty or the
//! first chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TOwningKey GetMinKeyOrThrow(const TChunkTree* chunkTree);

//! Returns the maximum key of a chunk. Throws if the chunk contains no boundary
//! info (i.e. it's not sorted).
NTableClient::TOwningKey GetMaxKeyOrThrow(const TChunk* chunk);

//! Returns the maximum key of a chunk tree. Throws if the tree is empty or the
//! last chunk in it contains no boundary info (i.e. it's not sorted).
//! Doesn't support chunk views.
NTableClient::TOwningKey GetMaxKeyOrThrow(const TChunkTree* chunkTree);

struct TChunkViewMergeResult;

std::vector<TChunkViewMergeResult> MergeAdjacentChunkViewRanges(std::vector<TChunkView*> chunkViews);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
