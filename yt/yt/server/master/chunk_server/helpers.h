#pragma once

#include "public.h"
#include "chunk_tree_statistics.h"
#include "chunk_replica.h"

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/journal_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Calls |functor(chunkList, child)| and |functor(parent(x), x)|, where |x|
//! iterates through proper ancestors of |chunkList|.
template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor, TChunkTree* child = nullptr);

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor);

template <class TRequest>
TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> ParseLocationDirectory(
    const IDataNodeTrackerPtr& dataNodeTracker,
    const TRequest& request);
template <class TRequest>
TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> ParseLocationDirectoryOrThrow(
    const TNode* node,
    const IDataNodeTrackerPtr& dataNodeTracker,
    const TRequest& request);

bool CanUnambiguouslyDetachChild(TChunkList* rootChunkList, const TChunkTree* child);

int GetChildIndex(const TChunkList* chunkList, const TChunkTree* child);

TChunkTree* FindFirstUnsealedChild(const TChunkList* chunkList);

i64 GetJournalChunkStartRowIndex(const TChunk* chunk);

TChunkList* GetUniqueParent(const TChunkTree* chunkTree);
TChunkList* GetUniqueParentOrThrow(const TChunkTree* chunkTree);
int GetParentCount(const TChunkTree* chunkTree);
bool HasParent(const TChunkTree* chunkTree, TChunkList* potentialParent);

void AttachToChunkList(
    TChunkList* chunkList,
    TRange<TChunkTree*> children);
void DetachFromChunkList(
    TChunkList* chunkList,
    TRange<TChunkTree*> children,
    EChunkDetachPolicy policy);

//! Set |childIndex|-th child of |chunkList| to |newChild|. It is up to caller
//! to deal with statistics.
void ReplaceChunkListChild(TChunkList* chunkList, int childIndex, TChunkTree* newChild);

void SetChunkTreeParent(TChunkTree* parent, TChunkTree* child);
void ResetChunkTreeParent(TChunkTree* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);
void AppendChunkTreeChild(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics);

//! Apply statisticsDelta to all proper ancestors of |child|.
//! Both statistics and cumulative statistics are updated.
//! |statisticsDelta| should have |child|'s rank.
void AccumulateAncestorsStatistics(
    TChunkTree* child,
    const TChunkTreeStatistics& statisticsDelta);
void AccumulateUniqueAncestorsStatistics(
    TChunkTree* child,
    const TChunkTreeStatistics& statisticsDelta);
void ResetChunkListStatistics(TChunkList* chunkList);
void RecomputeChunkListStatistics(TChunkList* chunkList);

void RecomputeChildToIndexMapping(TChunkList* chunkList);

std::vector<TChunkOwnerBase*> GetOwningNodes(
    TChunkTree* chunkTree);
TFuture<NYson::TYsonString> GetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTree* chunkTree);

void SerializeNodePath(
    NYson::IYsonConsumer* consumer,
    const NYPath::TYPath& path,
    TTransactionId transactionId);

bool IsEmpty(const TChunkList* chunkList);
bool IsEmpty(const TChunkTree* chunkTree);

//! Returns the upper boundary key of a chunk. Throws if the chunk contains no
//! boundary info (i.e. it's not sorted).
NTableClient::TLegacyOwningKey GetUpperBoundKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount = std::nullopt);

//! Returns the upper boundary key of a chunk tree. Throws if the tree is empty
//! or the last chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TLegacyOwningKey GetUpperBoundKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount = std::nullopt);

//! Returns upper key bound for a chunk tree. Throws if the tree is empty
//! or the last chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TOwningKeyBound GetUpperKeyBoundOrThrow(const TChunkTree* chunkTree, int keyColumnCount);

//! Returns the minimum key of a chunk. Throws if the chunk contains no boundary
//! info (i.e. it's not sorted).
NTableClient::TLegacyOwningKey GetMinKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount = std::nullopt);

//! Returns the minimum key of a chunk tree. Throws if the tree is empty or the
//! first chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TLegacyOwningKey GetMinKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount = std::nullopt);

//! Returns lower key bound for a chunk tree. Throws if the tree is empty or the
//! first chunk in it contains no boundary info (i.e. it's not sorted).
NTableClient::TOwningKeyBound GetLowerKeyBoundOrThrow(const TChunkTree* chunkTree, int keyColumnCount);

//! Returns the maximum key of a chunk. Throws if the chunk contains no boundary
//! info (i.e. it's not sorted).
NTableClient::TLegacyOwningKey GetMaxKeyOrThrow(const TChunk* chunk);

//! Returns minimum and maximum keys of a chunk. Throws if the chunk contains no boundary
// info (i.e. it's not sorted).
std::pair<NTableClient::TUnversionedOwningRow, NTableClient::TUnversionedOwningRow>
    GetBoundaryKeysOrThrow(const TChunk* chunk);

//! Returns the maximum key of a chunk tree. Throws if the tree is empty or the
//! last chunk in it contains no boundary info (i.e. it's not sorted).
//! Doesn't support chunk views.
NTableClient::TLegacyOwningKey GetMaxKeyOrThrow(const TChunkTree* chunkTree);

std::vector<TChunkViewMergeResult> MergeAdjacentChunkViewRanges(std::vector<TChunkView*> chunkViews);

std::vector<NJournalClient::TChunkReplicaDescriptor> GetChunkReplicaDescriptors(const TChunk* chunk);

void SerializeMediumDirectory(
    NChunkClient::NProto::TMediumDirectory* protoMediumDirectory,
    const IChunkManagerPtr& chunkManager);

void SerializeMediumOverrides(
    TNode* node,
    NDataNodeTrackerClient::NProto::TMediumOverrides* protoMediumOverrides);

int GetChunkShardIndex(TChunkId chunkId);

std::vector<TInstant> GenerateChunkCreationTimeHistogramBucketBounds(TInstant now);

TJobPtr MummifyJob(const TJobPtr& job);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
